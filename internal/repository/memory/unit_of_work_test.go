package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

func entry(eventID uuid.UUID) *domain.NotificationLog {
	return &domain.NotificationLog{ID: uuid.New(), EventID: eventID, Status: domain.StatusSent, CreatedAt: time.Now()}
}

func TestUnitOfWork_ConfirmaElRegistroSoloSiFnTerminaBien(t *testing.T) {
	logs := NewNotificationLogRepository()
	uow := NewUnitOfWork(logs, NewDeviceTokenRepository())
	eventID := uuid.New()

	err := uow.Do(context.Background(), func(tx domain.TxRepositories) error {
		if err := tx.NotificationLogs.Register(context.Background(), entry(eventID)); err != nil {
			return err
		}
		if _, visible := logs.Get(eventID); visible {
			t.Error("el registro no deberia ser visible antes de confirmar")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do() devolvio un error inesperado: %v", err)
	}
	if _, ok := logs.Get(eventID); !ok {
		t.Fatal("el registro deberia ser visible tras confirmar")
	}
}

func TestUnitOfWork_RevierteElRegistroSiFnFalla(t *testing.T) {
	logs := NewNotificationLogRepository()
	uow := NewUnitOfWork(logs, NewDeviceTokenRepository())
	eventID := uuid.New()
	boom := errors.New("fcm no disponible")

	err := uow.Do(context.Background(), func(tx domain.TxRepositories) error {
		_ = tx.NotificationLogs.Register(context.Background(), entry(eventID))
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do() = %v, se esperaba el error de fn", err)
	}
	if _, ok := logs.Get(eventID); ok {
		t.Fatal("el registro no deberia existir tras el rollback")
	}
	if err := logs.Register(context.Background(), entry(eventID)); err != nil {
		t.Fatalf("el event_id deberia poder registrarse de nuevo tras el rollback: %v", err)
	}
}

func TestUnitOfWork_UnPanicoLiberaLaReservaEnVezDeBloquearALosDuplicados(t *testing.T) {
	logs := NewNotificationLogRepository()
	uow := NewUnitOfWork(logs, NewDeviceTokenRepository())
	eventID := uuid.New()

	func() {
		defer func() { _ = recover() }()
		_ = uow.Do(context.Background(), func(tx domain.TxRepositories) error {
			_ = tx.NotificationLogs.Register(context.Background(), entry(eventID))
			panic("fallo inesperado")
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := logs.Register(ctx, entry(eventID)); err != nil {
		t.Fatalf("Register() tras un panico = %v: la reserva deberia haberse liberado", err)
	}
}

func TestUnitOfWork_LaEsperaPorUnEventIDEnVueloRespetaElContexto(t *testing.T) {
	logs := NewNotificationLogRepository()
	eventID := uuid.New()

	// Una transaccion abierta que nunca termina durante el test.
	tx := newTxNotificationLogs(logs)
	if err := tx.Register(context.Background(), entry(eventID)); err != nil {
		t.Fatalf("Register() inicial devolvio un error inesperado: %v", err)
	}
	defer tx.rollback()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := logs.Register(ctx, entry(eventID))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Register() con la reserva ocupada = %v, se esperaba context.DeadlineExceeded", err)
	}
}
