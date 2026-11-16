//go:build integration

// Pruebas de integracion del canal de email (US-13) contra un RabbitMQ real
// y un Mailpit real. Se excluyen del "go test ./..." comun.
//
// Requisitos, ademas de los de dlq_integration_test.go:
//
//   - TEST_SMTP_ADDR con host:puerto del SMTP de Mailpit (localhost:1025).
//
//   - TEST_MAILPIT_API con la URL base de la API de Mailpit
//     (http://127.0.0.1:8025).
//
//   - Ningun otro consumidor sobre notification.email-requested.q (por
//     ejemplo, el contenedor del servicio detenido).
//
//     TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
//     TEST_SMTP_ADDR=localhost:1025 TEST_MAILPIT_API=http://127.0.0.1:8025 \
//     go test -tags=integration -count=1 -run IntegracionEmail ./internal/delivery/worker/
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	smtpclient "github.com/matiasfelau/vinculo-vial-notification/internal/client/smtp"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	emailservice "github.com/matiasfelau/vinculo-vial-notification/internal/service/email"
)

const (
	emailWorkQueue  = "notification.email-requested.q"
	emailRetryQueue = "notification.email-requested.retry"
	emailDLQQueue   = "notification.email-requested.dlq"
	mailpitWait     = 20 * time.Second
)

func requireEnv(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s no esta definida: las pruebas de integracion del email requieren un Mailpit real", name)
	}
	return value
}

// mailpitMessage es lo que la API de Mailpit devuelve de un mensaje.
type mailpitMessage struct {
	ID   string `json:"ID"`
	From struct {
		Address string `json:"Address"`
	} `json:"From"`
	To []struct {
		Address string `json:"Address"`
	} `json:"To"`
	Subject string `json:"Subject"`
	Text    string `json:"Text"`
	HTML    string `json:"HTML"`
}

type mailpitClient struct {
	t    *testing.T
	base string
	http *http.Client
}

func newMailpitClient(t *testing.T) *mailpitClient {
	return &mailpitClient{t: t, base: strings.TrimRight(requireEnv(t, "TEST_MAILPIT_API"), "/"), http: &http.Client{Timeout: 5 * time.Second}}
}

func (m *mailpitClient) get(path string, out any) {
	m.t.Helper()

	response, err := m.http.Get(m.base + path)
	if err != nil {
		m.t.Fatalf("no se pudo consultar Mailpit (%s): %v", path, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		m.t.Fatalf("Mailpit respondio %d a %s", response.StatusCode, path)
	}
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		m.t.Fatalf("respuesta de Mailpit invalida (%s): %v", path, err)
	}
}

// awaitMessage espera el mensaje dirigido a recipient y lo devuelve entero.
func (m *mailpitClient) awaitMessage(recipient string) mailpitMessage {
	m.t.Helper()

	deadline := time.Now().Add(mailpitWait)
	for time.Now().Before(deadline) {
		var search struct {
			Messages []struct {
				ID string `json:"ID"`
			} `json:"messages"`
		}
		m.get("/api/v1/search?query=to:"+recipient, &search)
		if len(search.Messages) > 0 {
			var message mailpitMessage
			m.get("/api/v1/message/"+search.Messages[0].ID, &message)
			return message
		}
		time.Sleep(200 * time.Millisecond)
	}

	m.t.Fatalf("Mailpit no recibio ningun mensaje para %s en %s", recipient, mailpitWait)
	return mailpitMessage{}
}

func (m *mailpitClient) delete(id string) {
	body := strings.NewReader(`{"IDs": ["` + id + `"]}`)
	request, _ := http.NewRequest(http.MethodDelete, m.base+"/api/v1/messages", body)
	request.Header.Set("Content-Type", "application/json")
	if response, err := m.http.Do(request); err == nil {
		response.Body.Close()
	}
}

// publishAndConsume levanta un consumidor real sobre emailWorkQueue con el
// handler dado, publica body en la cola y devuelve una funcion que apaga
// todo. Publica por el exchange default con la cola como routing key.
func publishAndConsume(t *testing.T, handler messageHandler, body []byte) func() {
	t.Helper()

	url := integrationRabbitURL(t)
	conn, channel, err := dial(url, integrationPrefetch)
	if err != nil {
		t.Fatalf("no se pudo conectar el consumer al broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- NewConsumer(channel, emailWorkQueue, handler).Run(ctx) }()

	confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, "", emailWorkQueue, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		t.Fatalf("no se pudo publicar en %s: %v", emailWorkQueue, err)
	}
	if acked, err := confirmation.WaitContext(ctx); err != nil || !acked {
		t.Fatalf("el broker no confirmo la publicacion (acked=%v, err=%v)", acked, err)
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case <-runDone:
			case <-time.After(integrationShutdownMax):
				t.Error("el consumer no termino tras cancelar el contexto")
			}
			_ = channel.Close()
			_ = conn.Close()
		})
	}
}

// Recorre el camino real completo para cada plantilla: RabbitMQ -> consumer
// -> handler -> servicio -> adaptador SMTP -> Mailpit, y verifica lo que
// Mailpit recibio.
func TestIntegracionEmailRequested_EnviaCadaPlantillaARealMailpit(t *testing.T) {
	addr := requireEnv(t, "TEST_SMTP_ADDR")
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("TEST_SMTP_ADDR invalida: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("TEST_SMTP_ADDR con puerto invalido: %v", err)
	}
	mailpit := newMailpitClient(t)

	sender, err := smtpclient.NewSender(smtpclient.Config{
		Host:    host,
		Port:    port,
		From:    "Vinculo Vial <no-responder@vinculovial.local>",
		Timeout: 5 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("no se pudo construir el adaptador SMTP: %v", err)
	}

	cases := []struct {
		template string
		code     string
		expires  string
		subject  string
		contains []string
	}{
		{"EMAIL_VERIFICATION", "123456", inMinutes(10), "Tu código de Vinculo Vial", []string{"123456", "minutos", "ignorá este mail"}},
		{"ACCOUNT_EXISTS", "", "", "Ya tenés una cuenta en Vinculo Vial", []string{"ya tiene una cuenta", "recuperarla desde la app"}},
		{"PASSWORD_RESET", "654321", inMinutes(15), "Recuperá tu contraseña", []string{"654321", "nadie puede cambiar tu contraseña"}},
		{"PASSWORD_CHANGED", "", "", "Cambiaste tu contraseña", []string{"hora de Argentina", "se cerraron todas tus sesiones"}},
	}

	for _, tc := range cases {
		t.Run(tc.template, func(t *testing.T) {
			recipient := fmt.Sprintf("it-%s@example.com", uuid.NewString())
			service := emailservice.NewSendEmailService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()), sender)
			deadLetters := &fakeDeadLetterPublisher{}
			handler := NewEmailRequestedHandler(service, deadLetters, emailWorkQueue, integrationMaxRetries)

			stop := publishAndConsume(t, handler, emailEnvelope(uuid.New(), "", tc.template, recipient, tc.code, tc.expires))
			defer stop()

			message := mailpit.awaitMessage(recipient)
			defer mailpit.delete(message.ID)
			stop() // el consumer termina antes de leer lo que el handler pudo escribir
			if len(deadLetters.published) != 0 {
				t.Errorf("no deberia haberse publicado nada a la dlq, se publicaron %d", len(deadLetters.published))
			}

			if message.Subject != tc.subject {
				t.Errorf("Subject = %q, se esperaba %q", message.Subject, tc.subject)
			}
			if message.From.Address != "no-responder@vinculovial.local" {
				t.Errorf("From = %q", message.From.Address)
			}
			if len(message.To) != 1 || message.To[0].Address != recipient {
				t.Errorf("To = %+v, se esperaba %s", message.To, recipient)
			}
			for _, want := range tc.contains {
				if !strings.Contains(message.Text, want) {
					t.Errorf("el texto plano no contiene %q:\n%s", want, message.Text)
				}
				if !strings.Contains(message.HTML, want) {
					t.Errorf("el HTML no contiene %q:\n%s", want, message.HTML)
				}
			}
		})
	}
}

// Con el ciclo real de retry y DLQ de Core, un email.requested que nunca se
// puede enviar termina en la DLQ con to y code en null.
func TestIntegracionEmailRequested_AgotaReintentosYTerminaRedactadoEnLaDLQ(t *testing.T) {
	eventID := uuid.New()
	body := emailEnvelope(eventID, uuid.NewString(), "PASSWORD_RESET", secretTo, secretCode, inMinutes(15))

	newHandler := func(deadLetters deadLetterPublisher) messageHandler {
		sender := &fakeEmailSender{}
		for i := 0; i <= integrationMaxRetries; i++ {
			sender.errs = append(sender.errs, errors.New("smtp: conexion rechazada"))
		}
		service := emailservice.NewSendEmailService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()), sender)
		return NewEmailRequestedHandler(service, deadLetters, emailWorkQueue, integrationMaxRetries)
	}

	dead := runExhaustedRetriesScenario(t, emailWorkQueue, emailRetryQueue, emailDLQQueue, newHandler, body, eventID.String())

	assertRedactedEmailRequest(t, dead.Body, eventID, "PASSWORD_RESET")
}
