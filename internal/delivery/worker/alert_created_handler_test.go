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

type fakeDispatcher struct {
	err      error
	messages []domain.PushMessage
	tokens   [][]string
}

func (f *fakeDispatcher) Dispatch(_ context.Context, tokens []string, message domain.PushMessage) ([]domain.DispatchOutcome, error) {
	f.messages = append(f.messages, message)
	f.tokens = append(f.tokens, tokens)
	if f.err != nil {
		return nil, f.err
	}
	outcomes := make([]domain.DispatchOutcome, len(tokens))
	for i, token := range tokens {
		outcomes[i] = domain.DispatchOutcome{Token: token, Success: true}
	}
	return outcomes, nil
}

const (
	testAlertQueue    = "notification.alert-created.q"
	testSessionsQueue = "notification.sessions-revoked.q"
)

type fakeDeadLetterPublisher struct {
	published [][]byte
	queues    []string
	reasons   []string
	// err, si no es nil, simula que la DLQ no confirmo la publicacion
	// (broker caido, falta de ack de publisher confirm).
	err error
}

func (f *fakeDeadLetterPublisher) PublishToDLQ(_ context.Context, queue string, body []byte, reason string) error {
	f.published = append(f.published, body)
	f.queues = append(f.queues, queue)
	f.reasons = append(f.reasons, reason)
	return f.err
}

// assertDeadLetterQueue verifica que el unico mensaje descartado se
// haya publicado en la DLQ de la cola de trabajo esperada.
func assertDeadLetterQueue(t *testing.T, deadLetters *fakeDeadLetterPublisher, want string) {
	t.Helper()
	if len(deadLetters.queues) != 1 || deadLetters.queues[0] != want {
		t.Errorf("colas de origen = %v, se esperaba [%q]", deadLetters.queues, want)
	}
}

func validEnvelope(eventID, ownerID string) []byte {
	return []byte(`{
		"eventId": "` + eventID + `",
		"eventType": "alert.created",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-alert",
		"actor": "44444444-4444-4444-4444-444444444444",
		"payload": {"owner_ids": ["` + ownerID + `"], "alert_type": "choque"}
	}`)
}

func TestAlertCreatedHandler_ProcesaUnEventoValido_Ack(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{})
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)

	outcome := handler.Handle(context.Background(), validEnvelope(uuid.New().String(), ownerID.String()), 0)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	if len(deadLetters.published) != 0 {
		t.Fatalf("no deberia haberse publicado nada a la dlq, se publicaron %d mensajes", len(deadLetters.published))
	}
}

func TestAlertCreatedHandler_EnvelopeMalFormado_AckYDLQ(t *testing.T) {
	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()), memory.NewDeviceTokenRepository(), &fakeDispatcher{})
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)

	body := []byte(`{esto no es json valido`)
	outcome := handler.Handle(context.Background(), body, 0)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	if len(deadLetters.published) != 1 {
		t.Fatalf("se esperaba un mensaje publicado a la dlq, se publicaron %d", len(deadLetters.published))
	}
	if string(deadLetters.published[0]) != string(body) {
		t.Errorf("el cuerpo publicado a la dlq no coincide con el original")
	}
	if deadLetters.queues[0] != testAlertQueue {
		t.Errorf("cola de origen = %q, se esperaba %q", deadLetters.queues[0], testAlertQueue)
	}
}

func TestAlertCreatedHandler_EnvelopeMalFormadoYFalloAlPublicarEnLaDLQ_NackSinRequeue(t *testing.T) {
	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()), memory.NewDeviceTokenRepository(), &fakeDispatcher{})
	deadLetters := &fakeDeadLetterPublisher{err: errors.New("dlq no confirmo la publicacion")}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)

	body := []byte(`{esto no es json valido`)
	outcome := handler.Handle(context.Background(), body, 0)

	// Si la DLQ no confirma, el mensaje no debe perderse en silencio
	// con un ACK: se rechaza para que el ciclo de reintentos le de
	// otra oportunidad a la DLQ de estar disponible.
	if outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue cuando la dlq no confirma", outcome)
	}
}

func TestAlertCreatedHandler_PayloadSinOwnerIDs_AckYDLQ(t *testing.T) {
	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()), memory.NewDeviceTokenRepository(), &fakeDispatcher{})
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)

	body := []byte(`{
		"eventId": "11111111-1111-1111-1111-111111111111",
		"eventType": "alert.created",
		"payload": {"owner_ids": [], "alert_type": "choque"}
	}`)
	outcome := handler.Handle(context.Background(), body, 0)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	if len(deadLetters.published) != 1 {
		t.Fatalf("se esperaba un mensaje publicado a la dlq, se publicaron %d", len(deadLetters.published))
	}
	assertDeadLetterQueue(t, deadLetters, testAlertQueue)
}

func TestAlertCreatedHandler_FalloTransitorioDelDispatcher_NackSinRequeue(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{err: errors.New("fcm no disponible")})
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)

	outcome := handler.Handle(context.Background(), validEnvelope(uuid.New().String(), ownerID.String()), 0)

	if outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue", outcome)
	}
	if len(deadLetters.published) != 0 {
		t.Fatalf("un fallo transitorio no deberia publicar a la dlq, se publicaron %d mensajes", len(deadLetters.published))
	}
}

func TestAlertCreatedHandler_RedeliveryDelMismoEvento_Ack(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{})
	handler := NewAlertCreatedHandler(service, &fakeDeadLetterPublisher{}, testAlertQueue, 3)

	body := validEnvelope(uuid.New().String(), ownerID.String())

	first := handler.Handle(context.Background(), body, 0)
	second := handler.Handle(context.Background(), body, 0)

	if first != OutcomeAck || second != OutcomeAck {
		t.Fatalf("ambos intentos deberian confirmarse: primero=%v segundo=%v", first, second)
	}
}

func TestAlertCreatedHandler_FalloTransitorioBajoElLimite_NackSinRequeue(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{err: errors.New("fcm no disponible")})
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)

	body := validEnvelope(uuid.New().String(), ownerID.String())
	outcome := handler.Handle(context.Background(), body, 2)

	if outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue con retries (2) por debajo de maxRetries (3)", outcome)
	}
	if len(deadLetters.published) != 0 {
		t.Fatalf("no deberia haberse publicado a la dlq todavia, se publicaron %d mensajes", len(deadLetters.published))
	}
}

func TestAlertCreatedHandler_SuperaMaxRetries_AckYDLQ(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{err: errors.New("fcm no disponible")})
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)

	body := validEnvelope(uuid.New().String(), ownerID.String())
	outcome := handler.Handle(context.Background(), body, 3)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck al superar maxRetries", outcome)
	}
	if len(deadLetters.published) != 1 {
		t.Fatalf("se esperaba un mensaje publicado a la dlq al superar maxRetries, se publicaron %d", len(deadLetters.published))
	}
	if string(deadLetters.published[0]) != string(body) {
		t.Errorf("el cuerpo publicado a la dlq no coincide con el original")
	}
	if deadLetters.queues[0] != testAlertQueue {
		t.Errorf("cola de origen = %q, se esperaba %q", deadLetters.queues[0], testAlertQueue)
	}
}

func TestAlertCreatedHandler_SuperaMaxRetriesYFalloAlPublicarEnLaDLQ_NackSinRequeue(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	ownerID := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: ownerID, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := dispatch.NewDispatchAlertService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens), deviceTokens, &fakeDispatcher{err: errors.New("fcm no disponible")})
	deadLetters := &fakeDeadLetterPublisher{err: errors.New("dlq no confirmo la publicacion")}
	handler := NewAlertCreatedHandler(service, deadLetters, testAlertQueue, 3)

	body := validEnvelope(uuid.New().String(), ownerID.String())
	outcome := handler.Handle(context.Background(), body, 3)

	if outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue cuando la dlq no confirma al superar maxRetries", outcome)
	}
}
