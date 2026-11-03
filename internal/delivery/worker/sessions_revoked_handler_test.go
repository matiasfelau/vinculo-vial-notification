package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/sessions"
)

// failingDeviceTokenRepository envuelve el repositorio en memoria para
// forzar un error en DeleteByOwnerID sin necesitar una base real.
type failingDeviceTokenRepository struct {
	*memory.DeviceTokenRepository
	err error
}

func (f *failingDeviceTokenRepository) DeleteByOwnerID(context.Context, uuid.UUID) error {
	return f.err
}

func sessionsRevokedEnvelope(eventID, actorID string) []byte {
	return []byte(`{
		"eventId": "` + eventID + `",
		"eventType": "user.sessions.revoked",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-identity",
		"actor": "` + actorID + `",
		"payload": {"reason": "secuestro de sesion detectado"}
	}`)
}

func TestSessionsRevokedHandler_ProcesaUnEventoValido_BorraLosTokensYAck(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	owner := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens))
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewSessionsRevokedHandler(service, deadLetters, testSessionsQueue, 3)

	outcome := handler.Handle(context.Background(), sessionsRevokedEnvelope(uuid.New().String(), owner.String()), 0)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	restantes, _ := deviceTokens.FindByOwnerIDs(context.Background(), []uuid.UUID{owner})
	if len(restantes) != 0 {
		t.Fatalf("los tokens del owner deberian haberse borrado, se obtuvo: %+v", restantes)
	}
	if len(deadLetters.published) != 0 {
		t.Fatalf("no deberia haberse publicado nada a la dlq, se publicaron %d mensajes", len(deadLetters.published))
	}
}

// La baja de cuenta (US-11) no tiene evento propio para este servicio:
// Identity publica user.sessions.revoked junto con user.deleted, y este
// consumidor borra los tokens del usuario. Cubre todos sus dispositivos y
// no toca los de otros usuarios.
func TestSessionsRevokedHandler_BajaDeCuenta_BorraTodosLosTokensDelUsuarioYSoloLosSuyos(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	baja, otro := uuid.New(), uuid.New()
	for _, r := range []struct {
		owner uuid.UUID
		token string
	}{{baja, "celular"}, {baja, "tablet"}, {otro, "celular-ajeno"}} {
		if err := deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: r.owner, FCMToken: r.token, UpdatedAt: time.Now()}); err != nil {
			t.Fatalf("Upsert() devolvio un error inesperado: %v", err)
		}
	}

	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens))
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewSessionsRevokedHandler(service, deadLetters, testSessionsQueue, 3)

	body := []byte(`{
		"eventId": "` + uuid.New().String() + `",
		"eventType": "user.sessions.revoked",
		"timestamp": "2026-01-01T00:00:00Z",
		"producer": "vinculo-vial-identity",
		"actor": "` + baja.String() + `",
		"payload": {"reason": "account_deleted"}
	}`)
	if outcome := handler.Handle(context.Background(), body, 0); outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}

	delUsuario, _ := deviceTokens.FindByOwnerIDs(context.Background(), []uuid.UUID{baja})
	if len(delUsuario) != 0 {
		t.Errorf("los tokens del usuario dado de baja deberian haberse borrado, quedaron: %+v", delUsuario)
	}
	deOtro, _ := deviceTokens.FindByOwnerIDs(context.Background(), []uuid.UUID{otro})
	if len(deOtro) != 1 {
		t.Errorf("los tokens de otro usuario no deberian tocarse, quedaron: %+v", deOtro)
	}
	if len(deadLetters.published) != 0 {
		t.Errorf("no deberia haberse publicado nada a la dlq")
	}
}

func TestSessionsRevokedHandler_EnvelopeMalFormado_AckYDLQ(t *testing.T) {
	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()))
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewSessionsRevokedHandler(service, deadLetters, testSessionsQueue, 3)

	outcome := handler.Handle(context.Background(), []byte(`{no es json`), 0)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	if len(deadLetters.published) != 1 {
		t.Fatalf("se esperaba un mensaje publicado a la dlq, se publicaron %d", len(deadLetters.published))
	}
	assertDeadLetterQueue(t, deadLetters, testSessionsQueue)
}

func TestSessionsRevokedHandler_EnvelopeMalFormadoYFalloAlPublicarEnLaDLQ_NackSinRequeue(t *testing.T) {
	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()))
	deadLetters := &fakeDeadLetterPublisher{err: errors.New("dlq no confirmo la publicacion")}
	handler := NewSessionsRevokedHandler(service, deadLetters, testSessionsQueue, 3)

	outcome := handler.Handle(context.Background(), []byte(`{no es json`), 0)

	if outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue cuando la dlq no confirma", outcome)
	}
}

func TestSessionsRevokedHandler_ActorInvalido_AckYDLQ(t *testing.T) {
	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), memory.NewDeviceTokenRepository()))
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewSessionsRevokedHandler(service, deadLetters, testSessionsQueue, 3)

	body := sessionsRevokedEnvelope(uuid.New().String(), "no-es-un-uuid")
	outcome := handler.Handle(context.Background(), body, 0)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck", outcome)
	}
	if len(deadLetters.published) != 1 {
		t.Fatalf("se esperaba un mensaje publicado a la dlq, se publicaron %d", len(deadLetters.published))
	}
	assertDeadLetterQueue(t, deadLetters, testSessionsQueue)
}

func TestSessionsRevokedHandler_RedeliveryDelMismoEvento_SoloUnaBaja(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	owner := uuid.New()
	_ = deviceTokens.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1", UpdatedAt: time.Now()})

	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), deviceTokens))
	handler := NewSessionsRevokedHandler(service, &fakeDeadLetterPublisher{}, testSessionsQueue, 3)

	body := sessionsRevokedEnvelope(uuid.New().String(), owner.String())
	first := handler.Handle(context.Background(), body, 0)
	second := handler.Handle(context.Background(), body, 0)

	if first != OutcomeAck || second != OutcomeAck {
		t.Fatalf("ambos intentos deberian confirmarse: primero=%v segundo=%v", first, second)
	}
}

func TestSessionsRevokedHandler_FalloTransitorioBajoElLimite_NackSinRequeue(t *testing.T) {
	repo := &failingDeviceTokenRepository{err: errors.New("base caida")}
	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), repo))
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewSessionsRevokedHandler(service, deadLetters, testSessionsQueue, 3)

	body := sessionsRevokedEnvelope(uuid.New().String(), uuid.New().String())
	outcome := handler.Handle(context.Background(), body, 1)

	if outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue", outcome)
	}
	if len(deadLetters.published) != 0 {
		t.Fatalf("no deberia haberse publicado a la dlq todavia, se publicaron %d mensajes", len(deadLetters.published))
	}
}

func TestSessionsRevokedHandler_SuperaMaxRetries_AckYDLQ(t *testing.T) {
	repo := &failingDeviceTokenRepository{err: errors.New("base caida")}
	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), repo))
	deadLetters := &fakeDeadLetterPublisher{}
	handler := NewSessionsRevokedHandler(service, deadLetters, testSessionsQueue, 3)

	body := sessionsRevokedEnvelope(uuid.New().String(), uuid.New().String())
	outcome := handler.Handle(context.Background(), body, 3)

	if outcome != OutcomeAck {
		t.Fatalf("Outcome = %v, se esperaba OutcomeAck al superar maxRetries", outcome)
	}
	if len(deadLetters.published) != 1 {
		t.Fatalf("se esperaba un mensaje publicado a la dlq, se publicaron %d", len(deadLetters.published))
	}
	assertDeadLetterQueue(t, deadLetters, testSessionsQueue)
}

func TestSessionsRevokedHandler_SuperaMaxRetriesYFalloAlPublicarEnLaDLQ_NackSinRequeue(t *testing.T) {
	repo := &failingDeviceTokenRepository{err: errors.New("base caida")}
	service := sessions.NewRevokeSessionsService(memory.NewUnitOfWork(memory.NewNotificationLogRepository(), repo))
	deadLetters := &fakeDeadLetterPublisher{err: errors.New("dlq no confirmo la publicacion")}
	handler := NewSessionsRevokedHandler(service, deadLetters, testSessionsQueue, 3)

	body := sessionsRevokedEnvelope(uuid.New().String(), uuid.New().String())
	outcome := handler.Handle(context.Background(), body, 3)

	if outcome != OutcomeNackNoRequeue {
		t.Fatalf("Outcome = %v, se esperaba OutcomeNackNoRequeue cuando la dlq no confirma al superar maxRetries", outcome)
	}
}
