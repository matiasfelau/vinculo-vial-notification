package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

const testPendingDeliveredQueue = "notification.alert-pending-delivered.q"

func pendingDeliveredEnvelope(eventID, actorField, payload string) []byte {
	return []byte(`{
		"eventId": "` + eventID + `",
		"eventType": "alert.pending.delivered",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-alert"` + actorField + `,
		"payload": ` + payload + `
	}`)
}

type pendingDeliveredFixture struct {
	handler     *PendingDeliveredHandler
	dispatcher  *fakeDispatcher
	deadLetters *fakeDeadLetterPublisher
	tokens      *memory.DeviceTokenRepository
}

func newPendingDeliveredFixture(maxRetries int64) pendingDeliveredFixture {
	tokens := memory.NewDeviceTokenRepository()
	dispatcher := &fakeDispatcher{}
	deadLetters := &fakeDeadLetterPublisher{}
	service := dispatch.NewNotifyInboxService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), tokens), tokens, dispatcher)
	return pendingDeliveredFixture{
		handler:     NewPendingDeliveredHandler(service, deadLetters, testPendingDeliveredQueue, maxRetries),
		dispatcher:  dispatcher,
		deadLetters: deadLetters,
		tokens:      tokens,
	}
}

func (f pendingDeliveredFixture) addToken(t *testing.T, owner uuid.UUID, token string) {
	t.Helper()
	if err := f.tokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: token, UpdatedAt: time.Now()}); err != nil {
		t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
	}
}

func TestPendingDeliveredHandler_EventoValido_AvisaAlDuenoConElTextoDelContrato(t *testing.T) {
	f := newPendingDeliveredFixture(3)
	owner := uuid.New()
	f.addToken(t, owner, "token-dueno")
	f.addToken(t, uuid.New(), "token-ajeno")

	body := pendingDeliveredEnvelope(uuid.New().String(), "", `{"owner_ids": ["`+owner.String()+`"], "count": 2}`)
	if outcome := f.handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(f.dispatcher.tokens) != 1 || len(f.dispatcher.tokens[0]) != 1 || f.dispatcher.tokens[0][0] != "token-dueno" {
		t.Fatalf("el destinatario deberia ser solo el dueno, se despacho a %v", f.dispatcher.tokens)
	}
	message := f.dispatcher.messages[0]
	if message.Body != "Tenés alertas sin leer en el buzón." {
		t.Errorf("Body = %q", message.Body)
	}
	if message.Data["type"] != "INBOX_UPDATED" || len(message.Data) != 1 {
		t.Errorf("Data = %v, se esperaba solo type=INBOX_UPDATED", message.Data)
	}
	if len(f.deadLetters.published) != 0 {
		t.Errorf("no deberia haberse publicado nada a la dlq")
	}
}

func TestPendingDeliveredHandler_NuncaAvisaAlEmisor(t *testing.T) {
	f := newPendingDeliveredFixture(3)
	owner, issuer := uuid.New(), uuid.New()
	f.addToken(t, owner, "token-dueno")
	f.addToken(t, issuer, "token-emisor")

	// El contrato no trae actor; si llegara uno, tampoco recibe el aviso.
	body := pendingDeliveredEnvelope(uuid.New().String(), `, "actor": "`+issuer.String()+`"`, `{"owner_ids": ["`+owner.String()+`"], "count": 1}`)
	if outcome := f.handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(f.dispatcher.tokens) != 1 || len(f.dispatcher.tokens[0]) != 1 || f.dispatcher.tokens[0][0] != "token-dueno" {
		t.Fatalf("solo el dueno deberia recibir el aviso, se despacho a %v", f.dispatcher.tokens)
	}
}

func TestPendingDeliveredHandler_DuenoSinTokens_AckSinError(t *testing.T) {
	f := newPendingDeliveredFixture(3)
	f.addToken(t, uuid.New(), "token-ajeno")

	body := pendingDeliveredEnvelope(uuid.New().String(), "", `{"owner_ids": ["`+uuid.New().String()+`"], "count": 1}`)
	if outcome := f.handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(f.dispatcher.messages) != 0 {
		t.Errorf("no deberia haberse despachado nada, se hicieron %d envios", len(f.dispatcher.messages))
	}
	if len(f.deadLetters.published) != 0 {
		t.Errorf("no deberia haberse publicado nada a la dlq")
	}
}

func TestPendingDeliveredHandler_MensajeInvalido_AckYDLQDeSuPropiaCola(t *testing.T) {
	invalid := map[string][]byte{
		"envelope mal formado":  []byte(`{esto no es json valido`),
		"envelope sin eventId":  []byte(`{"eventType": "alert.pending.delivered", "payload": {"owner_ids": ["` + uuid.New().String() + `"]}}`),
		"payload sin owner_ids": pendingDeliveredEnvelope(uuid.New().String(), "", `{"count": 1}`),
		"owner_ids vacio":       pendingDeliveredEnvelope(uuid.New().String(), "", `{"owner_ids": [], "count": 0}`),
		"owner_id sin formato":  pendingDeliveredEnvelope(uuid.New().String(), "", `{"owner_ids": ["xyz"], "count": 1}`),
	}

	for name, body := range invalid {
		t.Run(name, func(t *testing.T) {
			f := newPendingDeliveredFixture(3)

			if outcome := f.handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
			}
			if len(f.deadLetters.published) != 1 || string(f.deadLetters.published[0]) != string(body) {
				t.Fatalf("se esperaba el cuerpo original publicado una vez a la dlq, se obtuvo %d publicaciones", len(f.deadLetters.published))
			}
			assertDeadLetterQueue(t, f.deadLetters, testPendingDeliveredQueue)
			if len(f.dispatcher.messages) != 0 {
				t.Errorf("un mensaje invalido no deberia despachar nada")
			}
		})
	}
}

func TestPendingDeliveredHandler_MensajeInvalidoYDLQSinConfirmar_NackSinRequeue(t *testing.T) {
	f := newPendingDeliveredFixture(3)
	f.deadLetters.err = errors.New("dlq no confirmo la publicacion")

	if outcome := f.handler.Handle(context.Background(), []byte(`{esto no es json valido`), 0); outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue cuando la dlq no confirma", outcome)
	}
}

func TestPendingDeliveredHandler_FalloTransitorio_NackSinRequeueHastaElLimite(t *testing.T) {
	f := newPendingDeliveredFixture(3)
	owner := uuid.New()
	f.addToken(t, owner, "token-dueno")
	f.dispatcher.err = errors.New("fcm no disponible")

	body := pendingDeliveredEnvelope(uuid.New().String(), "", `{"owner_ids": ["`+owner.String()+`"], "count": 1}`)
	if outcome := f.handler.Handle(context.Background(), body, 2); outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue con retries (2) por debajo de maxRetries (3)", outcome)
	}
	if len(f.deadLetters.published) != 0 {
		t.Errorf("no deberia haberse publicado a la dlq todavia, se publicaron %d", len(f.deadLetters.published))
	}
}

func TestPendingDeliveredHandler_SuperaMaxRetries_AckYDLQDeSuPropiaCola(t *testing.T) {
	f := newPendingDeliveredFixture(3)
	owner := uuid.New()
	f.addToken(t, owner, "token-dueno")
	f.dispatcher.err = errors.New("fcm no disponible")

	body := pendingDeliveredEnvelope(uuid.New().String(), "", `{"owner_ids": ["`+owner.String()+`"], "count": 1}`)
	if outcome := f.handler.Handle(context.Background(), body, 3); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck al superar MAX_RETRIES", outcome)
	}
	assertDeadLetterQueue(t, f.deadLetters, testPendingDeliveredQueue)
}
