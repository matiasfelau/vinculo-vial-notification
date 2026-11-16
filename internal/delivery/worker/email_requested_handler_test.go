package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	emailservice "github.com/matiasfelau/vinculo-vial-notification/internal/service/email"
)

const (
	testEmailQueue = "notification.email-requested.q"
	secretTo       = "persona.secreta@example.com"
	secretCode     = "483920"
)

type fakeEmailSender struct {
	mu       sync.Mutex
	messages []domain.EmailMessage
	// errs se consume de a uno por llamada; vacio significa exito.
	errs []error
}

func (f *fakeEmailSender) Send(_ context.Context, message domain.EmailMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.messages = append(f.messages, message)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return nil
}

func (f *fakeEmailSender) sent() []domain.EmailMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.EmailMessage(nil), f.messages...)
}

type emailFixture struct {
	handler     *EmailRequestedHandler
	sender      *fakeEmailSender
	deadLetters *fakeDeadLetterPublisher
	logs        *memory.NotificationLogRepository
}

func newEmailFixture(maxRetries int64) emailFixture {
	logs := memory.NewNotificationLogRepository()
	sender := &fakeEmailSender{}
	service := emailservice.NewSendEmailService(memory.NewUnitOfWork(logs, memory.NewDeviceTokenRepository()), sender)
	deadLetters := &fakeDeadLetterPublisher{}
	return emailFixture{
		handler:     NewEmailRequestedHandler(service, deadLetters, testEmailQueue, maxRetries),
		sender:      sender,
		deadLetters: deadLetters,
		logs:        logs,
	}
}

// emailEnvelope arma un email.requested con el contrato: actor puede ser
// "" (null) y code o expiresAt "" (null).
func emailEnvelope(eventID uuid.UUID, actor, template, to, code, expiresAt string) []byte {
	quote := func(s string) string {
		if s == "" {
			return "null"
		}
		return `"` + s + `"`
	}
	return []byte(fmt.Sprintf(`{
		"eventId": %q,
		"eventType": "email.requested",
		"timestamp": "2026-01-02T02:30:00Z",
		"producer": "vinculo-vial-identity",
		"actor": %s,
		"payload": {"template": %q, "to": %q, "code": %s, "expires_at": %s}
	}`, eventID.String(), quote(actor), template, to, quote(code), quote(expiresAt)))
}

func inMinutes(n int) string {
	return time.Now().Add(time.Duration(n) * time.Minute).UTC().Format(time.RFC3339)
}

func captureWorkerLogs(t *testing.T) func() string {
	t.Helper()

	var buf bytes.Buffer
	var mu sync.Mutex
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}))
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestEmailRequestedHandler_EnviaLaPlantillaCorrectaDeCadaCaso(t *testing.T) {
	userID := uuid.New().String()

	cases := []struct {
		name     string
		actor    string
		template string
		code     string
		expires  string
		subject  string
		contains []string
	}{
		{"EMAIL_VERIFICATION sin actor", "", "EMAIL_VERIFICATION", secretCode, inMinutes(10), "Tu código de Vinculo Vial", []string{secretCode, "minutos"}},
		{"ACCOUNT_EXISTS sin actor ni codigo", "", "ACCOUNT_EXISTS", "", "", "Ya tenés una cuenta en Vinculo Vial", []string{"ya tiene una cuenta", "recuperarla desde la app"}},
		{"PASSWORD_RESET con actor", userID, "PASSWORD_RESET", secretCode, inMinutes(15), "Recuperá tu contraseña", []string{secretCode, "nadie puede cambiar tu contraseña"}},
		{"PASSWORD_CHANGED con actor", userID, "PASSWORD_CHANGED", "", "", "Cambiaste tu contraseña", []string{"01/01/2026 a las 23:30", "se cerraron todas tus sesiones"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEmailFixture(3)

			outcome := f.handler.Handle(context.Background(), emailEnvelope(uuid.New(), tc.actor, tc.template, secretTo, tc.code, tc.expires), 0)

			if outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
			}
			sent := f.sender.sent()
			if len(sent) != 1 {
				t.Fatalf("se esperaba 1 envio, se obtuvieron %d", len(sent))
			}
			if sent[0].To != secretTo || sent[0].Subject != tc.subject {
				t.Errorf("To/Subject = %q / %q, se esperaba %q / %q", sent[0].To, sent[0].Subject, secretTo, tc.subject)
			}
			for _, want := range tc.contains {
				if !strings.Contains(sent[0].TextBody, want) || !strings.Contains(sent[0].HTMLBody, want) {
					t.Errorf("el cuerpo no contiene %q:\ntexto: %s\nhtml: %s", want, sent[0].TextBody, sent[0].HTMLBody)
				}
			}
			if len(f.deadLetters.published) != 0 {
				t.Errorf("no deberia haberse publicado nada a la dlq, se publicaron %d", len(f.deadLetters.published))
			}
		})
	}
}

func TestEmailRequestedHandler_VencidoSeConfirmaSinEnviarNiIrALaDLQ(t *testing.T) {
	f := newEmailFixture(3)

	outcome := f.handler.Handle(context.Background(), emailEnvelope(uuid.New(), "", "EMAIL_VERIFICATION", secretTo, secretCode, inMinutes(-1)), 0)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	if got := len(f.sender.sent()); got != 0 {
		t.Fatalf("un email vencido no debe enviarse, se enviaron %d", got)
	}
	if len(f.deadLetters.published) != 0 {
		t.Fatalf("un email vencido no va a la dlq, se publicaron %d", len(f.deadLetters.published))
	}
}

func TestEmailRequestedHandler_IdempotentePorEventID(t *testing.T) {
	f := newEmailFixture(3)
	body := emailEnvelope(uuid.New(), "", "EMAIL_VERIFICATION", secretTo, secretCode, inMinutes(10))

	for i := 0; i < 3; i++ {
		if outcome := f.handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
			t.Fatalf("entrega #%d: Outcome = %v, se esperaba OutcomeAck", i+1, outcome)
		}
	}

	if got := len(f.sender.sent()); got != 1 {
		t.Fatalf("se esperaba 1 envio pese a 3 entregas del mismo evento, se obtuvieron %d", got)
	}
}

func TestEmailRequestedHandler_FallaTransitoriaHaceNackYElReintentoEnvia(t *testing.T) {
	f := newEmailFixture(3)
	f.sender.errs = []error{errors.New("smtp: conexion rechazada")}
	body := emailEnvelope(uuid.New(), "", "EMAIL_VERIFICATION", secretTo, secretCode, inMinutes(10))

	if outcome := f.handler.Handle(context.Background(), body, 0); outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue ante una falla transitoria", outcome)
	}
	if len(f.deadLetters.published) != 0 {
		t.Fatal("una falla transitoria con reintentos disponibles no va a la dlq")
	}

	if outcome := f.handler.Handle(context.Background(), body, 1); outcome != OutcomeAck {
		t.Fatalf("reintento: Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	if got := len(f.sender.sent()); got != 2 {
		t.Fatalf("se esperaban 2 intentos de envio, se obtuvieron %d", got)
	}
}

func TestEmailRequestedHandler_RechazoPermanenteSeConfirmaSinDLQYSinDestinatarioEnLogs(t *testing.T) {
	logs := captureWorkerLogs(t)
	f := newEmailFixture(3)
	f.sender.errs = []error{fmt.Errorf("%w: codigo 550", domain.ErrEmailRejected)}

	outcome := f.handler.Handle(context.Background(), emailEnvelope(uuid.New(), "", "EMAIL_VERIFICATION", secretTo, secretCode, inMinutes(10)), 0)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck ante un rechazo permanente", outcome)
	}
	if len(f.deadLetters.published) != 0 {
		t.Fatalf("un rechazo permanente se ackea y se loguea, no va a la dlq (se publicaron %d)", len(f.deadLetters.published))
	}
	output := logs()
	if !strings.Contains(output, "rechazo de forma permanente") {
		t.Errorf("se esperaba un log del rechazo permanente:\n%s", output)
	}
	assertNoSensitiveData(t, output)
}

func TestEmailRequestedHandler_AgotarReintentosPublicaRedactadoEnLaDLQ(t *testing.T) {
	logs := captureWorkerLogs(t)
	f := newEmailFixture(3)
	f.sender.errs = []error{errors.New("smtp: conexion rechazada")}
	eventID := uuid.New()
	actor := uuid.New().String()
	body := emailEnvelope(eventID, actor, "PASSWORD_RESET", secretTo, secretCode, inMinutes(15))

	outcome := f.handler.Handle(context.Background(), body, 3)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck una vez publicado a la dlq", outcome)
	}
	assertDeadLetterQueue(t, f.deadLetters, testEmailQueue)
	assertRedactedEmailRequest(t, f.deadLetters.published[0], eventID, "PASSWORD_RESET")
	assertNoSensitiveData(t, strings.Join(f.deadLetters.reasons, " "))
	assertNoSensitiveData(t, logs())
}

func TestEmailRequestedHandler_PedidosInvalidosVanAlaDLQRedactadosSinReintentar(t *testing.T) {
	cases := []struct {
		name     string
		template string
		code     string
		expires  string
		to       string
	}{
		{"plantilla desconocida", "OTRA", secretCode, inMinutes(10), secretTo},
		{"codigo ausente", "EMAIL_VERIFICATION", "", inMinutes(10), secretTo},
		{"destinatario mal formado", "ACCOUNT_EXISTS", "", "", "esto-no-es-un-email"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWorkerLogs(t)
			f := newEmailFixture(3)
			eventID := uuid.New()

			outcome := f.handler.Handle(context.Background(), emailEnvelope(eventID, "", tc.template, tc.to, tc.code, tc.expires), 0)

			if outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck una vez publicado a la dlq", outcome)
			}
			if got := len(f.sender.sent()); got != 0 {
				t.Fatalf("un pedido invalido no debe enviarse, se enviaron %d", got)
			}
			assertDeadLetterQueue(t, f.deadLetters, testEmailQueue)
			assertRedactedEmailRequest(t, f.deadLetters.published[0], eventID, tc.template)
			assertNoSensitiveData(t, logs()+strings.Join(f.deadLetters.reasons, " "))
		})
	}
}

func TestEmailRequestedHandler_MensajesMalFormadosVanAlaDLQSinDatos(t *testing.T) {
	validPayload := `{"template": "EMAIL_VERIFICATION", "to": "` + secretTo + `", "code": "` + secretCode + `", "expires_at": "2099-01-01T00:00:00Z"}`
	eventID := uuid.New().String()

	cases := map[string][]byte{
		"json roto con datos":     []byte(`{"eventId": "` + eventID + `", "payload": {"to": "` + secretTo + `", "code": "` + secretCode + `"`),
		"sin eventId":             []byte(`{"eventType": "email.requested", "payload": ` + validPayload + `}`),
		"sin eventType":           []byte(`{"eventId": "` + eventID + `", "payload": ` + validPayload + `}`),
		"payload no es un objeto": []byte(`{"eventId": "` + eventID + `", "eventType": "email.requested", "payload": "` + secretTo + `"}`),
		"sin template":            []byte(`{"eventId": "` + eventID + `", "eventType": "email.requested", "payload": {"to": "` + secretTo + `", "code": "` + secretCode + `"}}`),
		"sin destinatario":        []byte(`{"eventId": "` + eventID + `", "eventType": "email.requested", "payload": {"template": "EMAIL_VERIFICATION", "code": "` + secretCode + `"}}`),
		"expires_at invalido":     []byte(`{"eventId": "` + eventID + `", "eventType": "email.requested", "payload": {"template": "EMAIL_VERIFICATION", "to": "` + secretTo + `", "code": "` + secretCode + `", "expires_at": "mañana"}}`),
		"code con tipo invalido":  []byte(`{"eventId": "` + eventID + `", "eventType": "email.requested", "payload": {"template": "EMAIL_VERIFICATION", "to": "` + secretTo + `", "code": 483920}}`),
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			logs := captureWorkerLogs(t)
			f := newEmailFixture(3)

			outcome := f.handler.Handle(context.Background(), body, 0)

			if outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck una vez publicado a la dlq", outcome)
			}
			if got := len(f.sender.sent()); got != 0 {
				t.Fatalf("no debe enviarse nada, se enviaron %d", got)
			}
			assertDeadLetterQueue(t, f.deadLetters, testEmailQueue)
			assertNoSensitiveData(t, string(f.deadLetters.published[0]))
			assertNoSensitiveData(t, logs()+strings.Join(f.deadLetters.reasons, " "))
		})
	}
}

func TestEmailRequestedHandler_SiLaDLQNoConfirmaNoAckea(t *testing.T) {
	f := newEmailFixture(3)
	f.deadLetters.err = errors.New("dlq no confirmo la publicacion")

	outcome := f.handler.Handle(context.Background(), emailEnvelope(uuid.New(), "", "OTRA", secretTo, "", ""), 0)

	if outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue si la dlq no confirma", outcome)
	}
}

func TestEmailRequestedHandler_NoLogueaDestinatarioNiCodigoEnElCaminoFeliz(t *testing.T) {
	logs := captureWorkerLogs(t)
	f := newEmailFixture(3)

	f.handler.Handle(context.Background(), emailEnvelope(uuid.New(), "", "EMAIL_VERIFICATION", secretTo, secretCode, inMinutes(10)), 0)

	output := logs()
	if output == "" {
		t.Fatal("se esperaba al menos el log del envio")
	}
	assertNoSensitiveData(t, output)
}

func TestRedactEmailRequest_PonePayloadToYCodeEnNullYConservaElResto(t *testing.T) {
	eventID := uuid.New()
	actor := uuid.New().String()
	body := emailEnvelope(eventID, actor, "PASSWORD_RESET", secretTo, secretCode, "2099-01-01T00:00:00Z")

	redacted := redactEmailRequest(body)

	assertRedactedEmailRequest(t, redacted, eventID, "PASSWORD_RESET")
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(redacted, &envelope); err != nil {
		t.Fatalf("el cuerpo redactado no es JSON: %v", err)
	}
	if string(envelope["actor"]) != `"`+actor+`"` {
		t.Errorf("actor = %s, se esperaba conservarlo", envelope["actor"])
	}
	var payload map[string]json.RawMessage
	_ = json.Unmarshal(envelope["payload"], &payload)
	if string(payload["expires_at"]) != `"2099-01-01T00:00:00Z"` {
		t.Errorf("expires_at = %s, se esperaba conservarlo", payload["expires_at"])
	}
}

func TestRedactEmailRequest_SinCodeNiToIgualmenteLosDejaEnNull(t *testing.T) {
	redacted := redactEmailRequest([]byte(`{"eventId": "x", "payload": {"template": "ACCOUNT_EXISTS"}}`))

	var envelope struct {
		Payload map[string]json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(redacted, &envelope); err != nil {
		t.Fatalf("el cuerpo redactado no es JSON: %v", err)
	}
	for _, key := range []string{"to", "code"} {
		if raw, ok := envelope.Payload[key]; !ok || string(raw) != "null" {
			t.Errorf("payload.%s = %s (presente=%v), se esperaba null", key, raw, ok)
		}
	}
}

func TestRedactEmailRequest_CuerpoIrredactableSeReemplazaPorUnObjetoVacio(t *testing.T) {
	for _, body := range []string{``, `no es json ` + secretTo, `null`, `[1,2]`, `{"payload": "` + secretTo + `"}`, `{"payload": null}`, `{}`} {
		if got := string(redactEmailRequest([]byte(body))); got != "{}" {
			t.Errorf("redactEmailRequest(%q) = %s, se esperaba {}", body, got)
		}
	}
}

// assertRedactedEmailRequest verifica que el cuerpo publicado en la DLQ
// tenga to y code en null, ningun dato sensible y el resto intacto.
func assertRedactedEmailRequest(t *testing.T, body []byte, eventID uuid.UUID, template string) {
	t.Helper()

	assertNoSensitiveData(t, string(body))

	var envelope struct {
		EventID string `json:"eventId"`
		Payload struct {
			Template string          `json:"template"`
			To       json.RawMessage `json:"to"`
			Code     json.RawMessage `json:"code"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("el cuerpo publicado en la dlq no es JSON: %v\n%s", err, body)
	}
	if envelope.EventID != eventID.String() {
		t.Errorf("eventId = %q, se esperaba conservarlo (%s)", envelope.EventID, eventID)
	}
	if envelope.Payload.Template != template {
		t.Errorf("template = %q, se esperaba conservarlo (%s)", envelope.Payload.Template, template)
	}
	if string(envelope.Payload.To) != "null" || string(envelope.Payload.Code) != "null" {
		t.Errorf("to = %s, code = %s: se esperaba null en ambos", envelope.Payload.To, envelope.Payload.Code)
	}
}

func assertNoSensitiveData(t *testing.T, text string) {
	t.Helper()

	for _, secret := range []string{secretTo, "persona.secreta", secretCode} {
		if strings.Contains(text, secret) {
			t.Errorf("el texto contiene un dato sensible (%q):\n%s", secret, text)
		}
	}
}
