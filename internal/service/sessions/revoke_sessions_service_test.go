package sessions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
)

func TestRevokeSessionsService_BorraTodosLosTokensDelOwner(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	notificationLogs := memory.NewNotificationLogRepository()
	service := NewRevokeSessionsService(notificationLogs, deviceTokens)
	ctx := context.Background()

	owner := uuid.New()
	otro := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d1", UpdatedAt: time.Now()})
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d2", UpdatedAt: time.Now()})
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: otro, FCMToken: "d3", UpdatedAt: time.Now()})

	event := SessionsRevokedEvent{EventID: uuid.New(), OwnerID: owner}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() devolvio un error inesperado: %v", err)
	}

	restantes, err := deviceTokens.FindByOwnerIDs(ctx, []uuid.UUID{owner, otro})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(restantes) != 1 || restantes[0].FCMToken != "d3" {
		t.Fatalf("solo deberia sobrevivir el token del otro owner, se obtuvo: %+v", restantes)
	}
}

func TestRevokeSessionsService_MismoEventIDEntregadoDosVeces_SoloUnaBaja(t *testing.T) {
	deviceTokens := memory.NewDeviceTokenRepository()
	notificationLogs := memory.NewNotificationLogRepository()
	service := NewRevokeSessionsService(notificationLogs, deviceTokens)
	ctx := context.Background()

	owner := uuid.New()
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d1", UpdatedAt: time.Now()})

	event := SessionsRevokedEvent{EventID: uuid.New(), OwnerID: owner}
	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del primer intento devolvio un error inesperado: %v", err)
	}

	// Un usuario podria registrar un token nuevo entre el primer
	// procesamiento y el redelivery: el redelivery no deberia
	// tocarlo, porque el evento ya fue procesado.
	_ = deviceTokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "d2", UpdatedAt: time.Now()})

	if err := service.Execute(ctx, event); err != nil {
		t.Fatalf("Execute() del redelivery devolvio un error inesperado: %v", err)
	}

	restantes, err := deviceTokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if err != nil {
		t.Fatalf("FindByOwnerIDs() devolvio un error inesperado: %v", err)
	}
	if len(restantes) != 1 || restantes[0].FCMToken != "d2" {
		t.Fatalf("el redelivery no deberia haber tocado el token registrado despues del primer procesamiento, se obtuvo: %+v", restantes)
	}
}

func TestRevokeSessionsService_PropagaElErrorDelBorrado(t *testing.T) {
	notificationLogs := memory.NewNotificationLogRepository()
	service := NewRevokeSessionsService(notificationLogs, &failingDeviceTokenRepository{err: errors.New("base caida")})

	event := SessionsRevokedEvent{EventID: uuid.New(), OwnerID: uuid.New()}
	if err := service.Execute(context.Background(), event); err == nil {
		t.Fatal("se esperaba que Execute() propagara el error del borrado")
	}
}

// failingDeviceTokenRepository envuelve el repositorio en memoria para
// forzar un error en DeleteByOwnerID sin necesitar una base real.
type failingDeviceTokenRepository struct {
	*memory.DeviceTokenRepository
	err error
}

func (f *failingDeviceTokenRepository) DeleteByOwnerID(context.Context, uuid.UUID) error {
	return f.err
}
