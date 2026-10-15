package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

const (
	testRejectedQueue = "notification.alert-rejected.q"
	testExpiredQueue  = "notification.alert-expired.q"
	testSeenQueue     = "notification.alert-seen.q"
)

// consumer describe uno de los tres consumidores de avisos al emisor:
// como construirlo, su cola, y el aviso que le corresponde.
type issuerConsumer struct {
	name      string
	eventType string
	queue     string
	body      string
	payload   string
	build     func(*dispatch.NotifyIssuerService, deadLetterPublisher, int64) *IssuerNoticeHandler
}

func issuerConsumers() []issuerConsumer {
	return []issuerConsumer{
		{
			name: "alert.rejected", eventType: "alert.rejected", queue: testRejectedQueue,
			body: "No pudimos validar tu alerta.",
			// reason_codes viaja en el evento real y nunca debe llegar a la push.
			payload: `{"alert_id": "%s", "reason_codes": ["PLATE_MISMATCH"]}`,
			build: func(s *dispatch.NotifyIssuerService, d deadLetterPublisher, retries int64) *IssuerNoticeHandler {
				return NewAlertRejectedHandler(s, d, testRejectedQueue, retries)
			},
		},
		{
			name: "alert.expired", eventType: "alert.expired", queue: testExpiredQueue,
			body:    "Tu alerta no tuvo respuesta.",
			payload: `{"alert_id": "%s", "alert_type": "CRASHED"}`,
			build: func(s *dispatch.NotifyIssuerService, d deadLetterPublisher, retries int64) *IssuerNoticeHandler {
				return NewAlertExpiredHandler(s, d, testExpiredQueue, retries)
			},
		},
		{
			name: "alert.seen", eventType: "alert.seen", queue: testSeenQueue,
			body:    "El dueño leyó tu mensaje.",
			payload: `{"alert_id": "%s", "alert_type": "PARKED_ON_GARAGE"}`,
			build: func(s *dispatch.NotifyIssuerService, d deadLetterPublisher, retries int64) *IssuerNoticeHandler {
				return NewAlertSeenHandler(s, d, testSeenQueue, retries)
			},
		},
	}
}

func (c issuerConsumer) envelope(eventID, actor, alertID string) []byte {
	return []byte(`{
		"eventId": "` + eventID + `",
		"eventType": "` + c.eventType + `",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-alert",
		"actor": "` + actor + `",
		"payload": ` + strings.Replace(c.payload, "%s", alertID, 1) + `
	}`)
}

type issuerFixture struct {
	handler     *IssuerNoticeHandler
	dispatcher  *fakeDispatcher
	deadLetters *fakeDeadLetterPublisher
	tokens      *memory.DeviceTokenRepository
}

func (c issuerConsumer) fixture(maxRetries int64) issuerFixture {
	tokens := memory.NewDeviceTokenRepository()
	dispatcher := &fakeDispatcher{}
	deadLetters := &fakeDeadLetterPublisher{}
	service := dispatch.NewNotifyIssuerService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), tokens), tokens, dispatcher)
	return issuerFixture{handler: c.build(service, deadLetters, maxRetries), dispatcher: dispatcher, deadLetters: deadLetters, tokens: tokens}
}

func (f issuerFixture) addToken(t *testing.T, owner uuid.UUID, token string) {
	t.Helper()
	if err := f.tokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: token, UpdatedAt: time.Now()}); err != nil {
		t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
	}
}

func TestIssuerNoticeHandler_EventoValido_AvisaAlActorConElTextoDelContrato(t *testing.T) {
	for _, c := range issuerConsumers() {
		t.Run(c.name, func(t *testing.T) {
			f := c.fixture(3)
			actor, alertID := uuid.New(), uuid.New()
			f.addToken(t, actor, "token-emisor")
			f.addToken(t, uuid.New(), "token-ajeno")

			outcome := f.handler.Handle(context.Background(), c.envelope(uuid.New().String(), actor.String(), alertID.String()), 0)

			if outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
			}
			if len(f.dispatcher.tokens) != 1 || len(f.dispatcher.tokens[0]) != 1 || f.dispatcher.tokens[0][0] != "token-emisor" {
				t.Fatalf("el destinatario deberia ser solo el actor, se despacho a %v", f.dispatcher.tokens)
			}
			message := f.dispatcher.messages[0]
			if message.Title != "ViVIAna" || message.Body != c.body {
				t.Errorf("mensaje = %q / %q, se esperaba ViVIAna / %q", message.Title, message.Body, c.body)
			}
			if message.Data["type"] != "SENT_UPDATED" || message.Data["alert_id"] != alertID.String() || len(message.Data) != 2 {
				t.Errorf("Data = %v, se esperaba type=SENT_UPDATED y alert_id", message.Data)
			}
			if strings.Contains(message.Body+message.Title, "PLATE_MISMATCH") || strings.Contains(message.Body+message.Title, "fraude") {
				t.Errorf("el aviso no debe filtrar el motivo: %+v", message)
			}
			if len(f.deadLetters.published) != 0 {
				t.Errorf("no deberia haberse publicado nada a la dlq, se publicaron %d", len(f.deadLetters.published))
			}
		})
	}
}

func TestIssuerNoticeHandler_ActorSinTokens_AckSinError(t *testing.T) {
	for _, c := range issuerConsumers() {
		t.Run(c.name, func(t *testing.T) {
			f := c.fixture(3)
			f.addToken(t, uuid.New(), "token-ajeno")

			outcome := f.handler.Handle(context.Background(), c.envelope(uuid.New().String(), uuid.New().String(), uuid.New().String()), 0)

			if outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
			}
			if len(f.dispatcher.messages) != 0 {
				t.Errorf("no deberia haberse despachado nada, se hicieron %d envios", len(f.dispatcher.messages))
			}
			if len(f.deadLetters.published) != 0 {
				t.Errorf("no deberia haberse publicado nada a la dlq, se publicaron %d", len(f.deadLetters.published))
			}
		})
	}
}

func TestIssuerNoticeHandler_MensajeInvalido_AckYDLQDeSuPropiaCola(t *testing.T) {
	invalid := map[string]func(c issuerConsumer) []byte{
		"envelope mal formado": func(issuerConsumer) []byte { return []byte(`{esto no es json valido`) },
		"envelope sin eventId": func(issuerConsumer) []byte {
			return []byte(`{"eventType": "alert.seen", "actor": "` + uuid.New().String() + `", "payload": {"alert_id": "` + uuid.New().String() + `"}}`)
		},
		"actor ausente": func(c issuerConsumer) []byte {
			return c.envelope(uuid.New().String(), "", uuid.New().String())
		},
		"actor que no es uuid": func(c issuerConsumer) []byte {
			return c.envelope(uuid.New().String(), "no-es-uuid", uuid.New().String())
		},
		"payload sin alert_id": func(c issuerConsumer) []byte {
			return []byte(`{"eventId": "` + uuid.New().String() + `", "eventType": "` + c.eventType + `", "actor": "` + uuid.New().String() + `", "payload": {}}`)
		},
		"alert_id que no es uuid": func(c issuerConsumer) []byte {
			return c.envelope(uuid.New().String(), uuid.New().String(), "xyz")
		},
	}

	for _, c := range issuerConsumers() {
		for name, build := range invalid {
			t.Run(c.name+"/"+name, func(t *testing.T) {
				f := c.fixture(3)
				body := build(c)

				outcome := f.handler.Handle(context.Background(), body, 0)

				if outcome != OutcomeAck {
					t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
				}
				if len(f.deadLetters.published) != 1 || string(f.deadLetters.published[0]) != string(body) {
					t.Fatalf("se esperaba el cuerpo original publicado una vez a la dlq, se obtuvo %d publicaciones", len(f.deadLetters.published))
				}
				assertDeadLetterQueue(t, f.deadLetters, c.queue)
				if len(f.dispatcher.messages) != 0 {
					t.Errorf("un mensaje invalido no deberia despachar nada")
				}
			})
		}
	}
}

func TestIssuerNoticeHandler_MensajeInvalidoYDLQSinConfirmar_NackSinRequeue(t *testing.T) {
	for _, c := range issuerConsumers() {
		t.Run(c.name, func(t *testing.T) {
			f := c.fixture(3)
			f.deadLetters.err = errors.New("dlq no confirmo la publicacion")

			outcome := f.handler.Handle(context.Background(), []byte(`{esto no es json valido`), 0)

			if outcome != OutcomeNackNoRequeue {
				t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue cuando la dlq no confirma", outcome)
			}
		})
	}
}

func TestIssuerNoticeHandler_FalloTransitorio_NackSinRequeueHastaElLimite(t *testing.T) {
	for _, c := range issuerConsumers() {
		t.Run(c.name, func(t *testing.T) {
			f := c.fixture(3)
			actor := uuid.New()
			f.addToken(t, actor, "token-emisor")
			f.dispatcher.err = errors.New("fcm no disponible")

			outcome := f.handler.Handle(context.Background(), c.envelope(uuid.New().String(), actor.String(), uuid.New().String()), 2)

			if outcome != OutcomeNackNoRequeue {
				t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue con retries (2) por debajo de maxRetries (3)", outcome)
			}
			if len(f.deadLetters.published) != 0 {
				t.Errorf("no deberia haberse publicado a la dlq todavia, se publicaron %d", len(f.deadLetters.published))
			}
		})
	}
}

func TestIssuerNoticeHandler_SuperaMaxRetries_AckYDLQDeSuPropiaCola(t *testing.T) {
	for _, c := range issuerConsumers() {
		t.Run(c.name, func(t *testing.T) {
			f := c.fixture(3)
			actor := uuid.New()
			f.addToken(t, actor, "token-emisor")
			f.dispatcher.err = errors.New("fcm no disponible")
			body := c.envelope(uuid.New().String(), actor.String(), uuid.New().String())

			outcome := f.handler.Handle(context.Background(), body, 3)

			if outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck al superar maxRetries", outcome)
			}
			if len(f.deadLetters.published) != 1 || string(f.deadLetters.published[0]) != string(body) {
				t.Fatalf("se esperaba el cuerpo original publicado una vez a la dlq, se obtuvo %d publicaciones", len(f.deadLetters.published))
			}
			assertDeadLetterQueue(t, f.deadLetters, c.queue)
		})
	}
}

func TestIssuerNoticeHandler_SuperaMaxRetriesYDLQSinConfirmar_NackSinRequeue(t *testing.T) {
	for _, c := range issuerConsumers() {
		t.Run(c.name, func(t *testing.T) {
			f := c.fixture(3)
			actor := uuid.New()
			f.addToken(t, actor, "token-emisor")
			f.dispatcher.err = errors.New("fcm no disponible")
			f.deadLetters.err = errors.New("dlq no confirmo la publicacion")

			outcome := f.handler.Handle(context.Background(), c.envelope(uuid.New().String(), actor.String(), uuid.New().String()), 3)

			if outcome != OutcomeNackNoRequeue {
				t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue", outcome)
			}
		})
	}
}

func TestIssuerNoticeHandler_RedeliveryDelMismoEvento_UnSoloAviso(t *testing.T) {
	for _, c := range issuerConsumers() {
		t.Run(c.name, func(t *testing.T) {
			f := c.fixture(3)
			actor := uuid.New()
			f.addToken(t, actor, "token-emisor")
			body := c.envelope(uuid.New().String(), actor.String(), uuid.New().String())

			first := f.handler.Handle(context.Background(), body, 0)
			second := f.handler.Handle(context.Background(), body, 0)

			if first != OutcomeAck || second != OutcomeAck {
				t.Fatalf("ambos intentos deberian confirmarse: primero=%v segundo=%v", first, second)
			}
			if len(f.dispatcher.messages) != 1 {
				t.Fatalf("se esperaba un unico aviso, se enviaron %d", len(f.dispatcher.messages))
			}
		})
	}
}

// Un test por consumidor que fija su cola y su aviso: si alguien cruza
// los constructores, falla el que corresponde.

func TestAlertRejectedHandler_UsaSuColaYSuTexto(t *testing.T) {
	assertConsumerWiring(t, NewAlertRejectedHandler, testRejectedQueue, "No pudimos validar tu alerta.", `{"alert_id": "%s", "reason_codes": ["NO_VEHICLE"]}`)
}

func TestAlertExpiredHandler_UsaSuColaYSuTexto(t *testing.T) {
	assertConsumerWiring(t, NewAlertExpiredHandler, testExpiredQueue, "Tu alerta no tuvo respuesta.", `{"alert_id": "%s", "alert_type": "CRASHED"}`)
}

func TestAlertSeenHandler_UsaSuColaYSuTexto(t *testing.T) {
	assertConsumerWiring(t, NewAlertSeenHandler, testSeenQueue, "El dueño leyó tu mensaje.", `{"alert_id": "%s", "alert_type": "CRASHED"}`)
}

func assertConsumerWiring(t *testing.T, build func(*dispatch.NotifyIssuerService, deadLetterPublisher, string, int64) *IssuerNoticeHandler, queue, wantBody, payload string) {
	t.Helper()

	tokens := memory.NewDeviceTokenRepository()
	dispatcher := &fakeDispatcher{}
	deadLetters := &fakeDeadLetterPublisher{}
	service := dispatch.NewNotifyIssuerService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), tokens), tokens, dispatcher)
	handler := build(service, deadLetters, queue, 3)

	actor := uuid.New()
	_ = tokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: actor, FCMToken: "token-emisor", UpdatedAt: time.Now()})

	valid := []byte(`{"eventId": "` + uuid.New().String() + `", "eventType": "x", "actor": "` + actor.String() + `", "payload": ` + strings.Replace(payload, "%s", uuid.New().String(), 1) + `}`)
	if outcome := handler.Handle(context.Background(), valid, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	if len(dispatcher.messages) != 1 || dispatcher.messages[0].Body != wantBody {
		t.Fatalf("mensajes = %+v, se esperaba uno con cuerpo %q", dispatcher.messages, wantBody)
	}

	if outcome := handler.Handle(context.Background(), []byte(`{invalido`), 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	assertDeadLetterQueue(t, deadLetters, queue)
}
