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
	if dispatcher.tokens[1][0] != "token-emisor" || dispatcher.messages[1].Body != "Tu alerta fue validada. Le avisé al dueño." {
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
