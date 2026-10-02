package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

func newLogEntry(eventID uuid.UUID) *domain.NotificationLog {
	return &domain.NotificationLog{ID: uuid.New(), EventID: eventID, Status: domain.StatusSent, CreatedAt: time.Now()}
}

func TestUnitOfWork_ConfirmaElRegistroYSusEfectosSiFnTerminaBien(t *testing.T) {
	db := openTestDB(t)
	uow := NewUnitOfWork(db, time.Second)
	ctx := context.Background()

	owner := uuid.New()
	tokens := NewDeviceTokenRepository(db, time.Second)
	_ = tokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "t1", UpdatedAt: time.Now()})

	eventID := uuid.New()
	err := uow.Do(ctx, func(tx domain.TxRepositories) error {
		if err := tx.NotificationLogs.Register(ctx, newLogEntry(eventID)); err != nil {
			return err
		}
		return tx.DeviceTokens.DeleteByOwnerID(ctx, owner)
	})
	if err != nil {
		t.Fatalf("Do() devolvio un error inesperado: %v", err)
	}

	restantes, _ := tokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if len(restantes) != 0 {
		t.Fatalf("el borrado deberia haberse confirmado, quedaron: %+v", restantes)
	}

	// Confirmado el event_id, una segunda entrega es un duplicado.
	logs := NewNotificationLogRepository(db, time.Second)
	if err := logs.Register(ctx, newLogEntry(eventID)); !errors.Is(err, domain.ErrEventAlreadyProcessed) {
		t.Fatalf("Register() de un event_id confirmado = %v, se esperaba ErrEventAlreadyProcessed", err)
	}
}

func TestUnitOfWork_RevierteElRegistroYSusEfectosSiFnFalla(t *testing.T) {
	db := openTestDB(t)
	uow := NewUnitOfWork(db, time.Second)
	ctx := context.Background()

	owner := uuid.New()
	tokens := NewDeviceTokenRepository(db, time.Second)
	_ = tokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "t1", UpdatedAt: time.Now()})

	eventID := uuid.New()
	fcmCaido := errors.New("fcm no disponible")
	err := uow.Do(ctx, func(tx domain.TxRepositories) error {
		if err := tx.NotificationLogs.Register(ctx, newLogEntry(eventID)); err != nil {
			return err
		}
		if err := tx.DeviceTokens.DeleteByOwnerID(ctx, owner); err != nil {
			return err
		}
		return fcmCaido
	})
	if !errors.Is(err, fcmCaido) {
		t.Fatalf("Do() = %v, se esperaba el error de fn", err)
	}

	// Rollback del efecto...
	restantes, _ := tokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if len(restantes) != 1 {
		t.Fatalf("el borrado deberia haberse revertido, se obtuvo: %+v", restantes)
	}

	// ...y del registro: el reintento no es un duplicado.
	logs := NewNotificationLogRepository(db, time.Second)
	if err := logs.Register(ctx, newLogEntry(eventID)); err != nil {
		t.Fatalf("Register() tras el rollback = %v, se esperaba nil", err)
	}
}

func TestUnitOfWork_DuplicadoDevuelveErrEventAlreadyProcessedYNoTocaNada(t *testing.T) {
	db := openTestDB(t)
	uow := NewUnitOfWork(db, time.Second)
	ctx := context.Background()

	eventID := uuid.New()
	logs := NewNotificationLogRepository(db, time.Second)
	if err := logs.Register(ctx, newLogEntry(eventID)); err != nil {
		t.Fatalf("Register() inicial devolvio un error inesperado: %v", err)
	}

	owner := uuid.New()
	tokens := NewDeviceTokenRepository(db, time.Second)
	_ = tokens.Upsert(ctx, &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "t1", UpdatedAt: time.Now()})

	err := uow.Do(ctx, func(tx domain.TxRepositories) error {
		if err := tx.NotificationLogs.Register(ctx, newLogEntry(eventID)); err != nil {
			return err
		}
		return tx.DeviceTokens.DeleteByOwnerID(ctx, owner)
	})
	if !errors.Is(err, domain.ErrEventAlreadyProcessed) {
		t.Fatalf("Do() = %v, se esperaba ErrEventAlreadyProcessed", err)
	}

	restantes, _ := tokens.FindByOwnerIDs(ctx, []uuid.UUID{owner})
	if len(restantes) != 1 {
		t.Fatalf("un duplicado no deberia tocar los tokens, se obtuvo: %+v", restantes)
	}
}
