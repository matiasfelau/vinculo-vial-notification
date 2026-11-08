package worker

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/dispatch"
)

func alertCreatedEnvelope(eventID, actor, ownerID, extraPayload string) []byte {
	return []byte(`{
		"eventId": "` + eventID + `",
		"eventType": "alert.created",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-alert",
		"actor": "` + actor + `",
		"payload": {"owner_ids": ["` + ownerID + `"], "alert_type": "CRASHED"` + extraPayload + `}
	}`)
}

// newAlertCreatedFixture arma un handler con tokens registrados para el
// dueno y el emisor indicados.
func newAlertCreatedFixture(t *testing.T, owner, issuer uuid.UUID) (*AlertCreatedHandler, *fakeDispatcher, *fakeDeadLetterPublisher) {
	t.Helper()
	deviceTokens := memory.NewDeviceTokenRepository()
	registrations := []struct {
		owner uuid.UUID
		token string
	}{{owner, "token-dueno"}, {issuer, "token-emisor"}}
	for _, r := range registrations {
		if err := deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: r.owner, FCMToken: r.token, UpdatedAt: time.Now()}); err != nil {
			t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
		}
	}
	dispatcher := &fakeDispatcher{}
	deadLetters := &fakeDeadLetterPublisher{}
	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, dispatcher)
	return NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3), dispatcher, deadLetters
}

func TestAlertCreatedHandler_ConAlertID_AvisaALosDuenosYAlEmisorConDeepLink(t *testing.T) {
	owner, issuer, alertID := uuid.New(), uuid.New(), uuid.New()
	handler, dispatcher, deadLetters := newAlertCreatedFixture(t, owner, issuer)

	body := alertCreatedEnvelope(uuid.New().String(), issuer.String(), owner.String(), `, "alert_id": "`+alertID.String()+`"`)
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 2 {
		t.Fatalf("se esperaban 2 avisos (duenos y emisor), se enviaron %d", len(dispatcher.messages))
	}
	if dispatcher.tokens[0][0] != "token-dueno" || dispatcher.messages[0].Data["alert_id"] != alertID.String() {
		t.Errorf("aviso a los duenos incorrecto: tokens=%v message=%+v", dispatcher.tokens[0], dispatcher.messages[0])
	}
	if dispatcher.tokens[1][0] != "token-emisor" || dispatcher.messages[1].Body != "Tu alerta fue validada." {
		t.Errorf("aviso al emisor incorrecto: tokens=%v message=%+v", dispatcher.tokens[1], dispatcher.messages[1])
	}
	if dispatcher.messages[1].Data["type"] != "SENT_UPDATED" || dispatcher.messages[1].Data["alert_id"] != alertID.String() {
		t.Errorf("Data del aviso al emisor = %v", dispatcher.messages[1].Data)
	}
	if len(deadLetters.published) != 0 {
		t.Errorf("no deberia haberse publicado nada a la dlq")
	}
}

func TestAlertCreatedHandler_PayloadSinAlertID_NoMandaDataDeDeepLink(t *testing.T) {
	owner, issuer := uuid.New(), uuid.New()
	handler, dispatcher, _ := newAlertCreatedFixture(t, owner, issuer)

	body := alertCreatedEnvelope(uuid.New().String(), issuer.String(), owner.String(), "")
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 2 {
		t.Fatalf("se esperaban 2 avisos, se enviaron %d", len(dispatcher.messages))
	}
	if _, ok := dispatcher.messages[0].Data["alert_id"]; ok {
		t.Errorf("el aviso a los duenos no deberia llevar alert_id: %v", dispatcher.messages[0].Data)
	}
	if len(dispatcher.messages[1].Data) != 0 {
		t.Errorf("el aviso al emisor no deberia llevar data de deep link: %v", dispatcher.messages[1].Data)
	}
}

func TestAlertCreatedHandler_ActorInvalido_AvisaSoloALosDuenosYHaceAck(t *testing.T) {
	owner, issuer := uuid.New(), uuid.New()
	handler, dispatcher, deadLetters := newAlertCreatedFixture(t, owner, issuer)

	body := alertCreatedEnvelope(uuid.New().String(), "no-es-uuid", owner.String(), "")
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 1 || dispatcher.tokens[0][0] != "token-dueno" {
		t.Fatalf("se esperaba un unico aviso a los duenos, se obtuvo tokens=%v", dispatcher.tokens)
	}
	if len(deadLetters.published) != 0 {
		t.Errorf("un actor invalido no deberia descartar el evento a la dlq")
	}
}

func TestAlertCreatedHandler_AlertIDMalFormado_AckYDLQ(t *testing.T) {
	owner, issuer := uuid.New(), uuid.New()
	handler, dispatcher, deadLetters := newAlertCreatedFixture(t, owner, issuer)

	body := alertCreatedEnvelope(uuid.New().String(), issuer.String(), owner.String(), `, "alert_id": "xyz"`)
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 0 {
		t.Errorf("un payload invalido no deberia despachar nada")
	}
	assertDeadLetterQueue(t, deadLetters, testAlertQueue)
}

func TestAlertCreatedHandler_EmisorSinTokens_AckSinError(t *testing.T) {
	owner, issuer := uuid.New(), uuid.New()
	// Los tokens se registran para otro usuario: el emisor real no tiene.
	handler, dispatcher, deadLetters := newAlertCreatedFixture(t, owner, uuid.New())

	body := alertCreatedEnvelope(uuid.New().String(), issuer.String(), owner.String(), `, "alert_id": "`+uuid.New().String()+`"`)
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 1 {
		t.Fatalf("se esperaba un unico aviso (a los duenos), se enviaron %d", len(dispatcher.messages))
	}
	if len(deadLetters.published) != 0 {
		t.Errorf("no deberia haberse publicado nada a la dlq")
	}
}

func alertCreatedEnvelopeSinDuenos(eventID, actor, extraPayload string) []byte {
	return []byte(`{
		"eventId": "` + eventID + `",
		"eventType": "alert.created",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-alert",
		"actor": "` + actor + `",
		"payload": {"owner_ids": [], "alert_type": "CRASHED"` + extraPayload + `}
	}`)
}

func TestAlertCreatedHandler_SinDuenos_AvisaSoloAlEmisorSinDLQ(t *testing.T) {
	owner, issuer, alertID := uuid.New(), uuid.New(), uuid.New()
	handler, dispatcher, deadLetters := newAlertCreatedFixture(t, owner, issuer)

	body := alertCreatedEnvelopeSinDuenos(uuid.New().String(), issuer.String(), `, "alert_id": "`+alertID.String()+`"`)
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 1 || len(dispatcher.tokens[0]) != 1 || dispatcher.tokens[0][0] != "token-emisor" {
		t.Fatalf("se esperaba un unico aviso al emisor, se obtuvo tokens=%v", dispatcher.tokens)
	}
	message := dispatcher.messages[0]
	if message.Title != "ViVIAna" || message.Body != "Tu alerta fue validada." {
		t.Errorf("mensaje = %q / %q", message.Title, message.Body)
	}
	if message.Data["type"] != "SENT_UPDATED" || message.Data["alert_id"] != alertID.String() || len(message.Data) != 2 {
		t.Errorf("Data = %v, se esperaba type=SENT_UPDATED y alert_id", message.Data)
	}
	if len(deadLetters.published) != 0 {
		t.Errorf("una alerta sin duenos y con actor es valida: no deberia ir a la dlq")
	}
}

func TestAlertCreatedHandler_SinDuenosYActorNoUUID_AckYDLQ(t *testing.T) {
	handler, dispatcher, deadLetters := newAlertCreatedFixture(t, uuid.New(), uuid.New())

	body := alertCreatedEnvelopeSinDuenos(uuid.New().String(), "no-es-uuid", "")
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 0 {
		t.Errorf("no deberia despacharse nada: no hay a quien avisar")
	}
	if len(deadLetters.published) != 1 || string(deadLetters.published[0]) != string(body) {
		t.Fatalf("se esperaba el cuerpo original publicado una vez a la dlq, se obtuvo %d publicaciones", len(deadLetters.published))
	}
	assertDeadLetterQueue(t, deadLetters, testAlertQueue)
}

// actorAusente son las tres formas en que un envelope puede venir sin
// actor: clave vacia, null o directamente sin la clave.
func actorAusente() map[string]string {
	return map[string]string{
		"actor vacio":     `"actor": "",`,
		"actor null":      `"actor": null,`,
		"sin clave actor": ``,
	}
}

func alertCreatedEnvelopeActorCrudo(eventID, actorField, ownersJSON string) []byte {
	return []byte(`{
		"eventId": "` + eventID + `",
		"eventType": "alert.created",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-alert",
		` + actorField + `
		"payload": {"owner_ids": ` + ownersJSON + `, "alert_type": "CRASHED", "alert_id": "` + uuid.New().String() + `"}
	}`)
}

func TestAlertCreatedHandler_SinDuenosYSinActor_AckSinPushNiDLQ(t *testing.T) {
	for name, actorField := range actorAusente() {
		t.Run(name, func(t *testing.T) {
			handler, dispatcher, deadLetters := newAlertCreatedFixture(t, uuid.New(), uuid.New())

			body := alertCreatedEnvelopeActorCrudo(uuid.New().String(), actorField, `[]`)
			if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
			}

			if len(dispatcher.messages) != 0 {
				t.Errorf("no deberia despacharse nada: el emisor se dio de baja y no hay duenos")
			}
			if len(deadLetters.published) != 0 {
				t.Errorf("la falta de actor no deberia llevar a la dlq, se publicaron %d mensajes", len(deadLetters.published))
			}
		})
	}
}

func TestAlertCreatedHandler_ConDuenosYSinActor_AvisaSoloALosDuenos(t *testing.T) {
	for name, actorField := range actorAusente() {
		t.Run(name, func(t *testing.T) {
			owner := uuid.New()
			handler, dispatcher, deadLetters := newAlertCreatedFixture(t, owner, uuid.New())

			body := alertCreatedEnvelopeActorCrudo(uuid.New().String(), actorField, `["`+owner.String()+`"]`)
			if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
				t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
			}

			if len(dispatcher.messages) != 1 || len(dispatcher.tokens[0]) != 1 || dispatcher.tokens[0][0] != "token-dueno" {
				t.Fatalf("se esperaba un unico aviso a los duenos, se obtuvo tokens=%v", dispatcher.tokens)
			}
			if dispatcher.messages[0].Data["type"] == "SENT_UPDATED" {
				t.Errorf("el aviso a los duenos no puede ser el del emisor: %+v", dispatcher.messages[0])
			}
			if len(deadLetters.published) != 0 {
				t.Errorf("la falta de actor no deberia llevar a la dlq, se publicaron %d mensajes", len(deadLetters.published))
			}
		})
	}
}

func TestAlertCreatedHandler_SinActorYPayloadInvalido_AckYDLQ(t *testing.T) {
	handler, dispatcher, deadLetters := newAlertCreatedFixture(t, uuid.New(), uuid.New())

	body := []byte(`{"eventId": "` + uuid.New().String() + `", "eventType": "alert.created", "payload": {"owner_ids": [], "alert_type": "CRASHED", "alert_id": "xyz"}}`)
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 0 {
		t.Errorf("un payload invalido no deberia despachar nada")
	}
	assertDeadLetterQueue(t, deadLetters, testAlertQueue)
}

func TestAlertCreatedHandler_SinDuenos_EmisorSinTokens_AckSinError(t *testing.T) {
	issuer := uuid.New()
	// Los tokens son de otros usuarios: el emisor real no tiene.
	handler, dispatcher, deadLetters := newAlertCreatedFixture(t, uuid.New(), uuid.New())

	body := alertCreatedEnvelopeSinDuenos(uuid.New().String(), issuer.String(), `, "alert_id": "`+uuid.New().String()+`"`)
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	if len(dispatcher.messages) != 0 {
		t.Errorf("no deberia despacharse nada")
	}
	if len(deadLetters.published) != 0 {
		t.Errorf("no deberia haberse publicado nada a la dlq")
	}
}
