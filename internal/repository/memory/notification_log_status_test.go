package memory

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

func TestNotificationLogRepository_UpdateStatusDeUnEventoInexistenteNoCreaNiFalla(t *testing.T) {
	repo := NewNotificationLogRepository()
	eventID := uuid.New()

	if err := repo.UpdateStatus(context.Background(), eventID, domain.StatusSent); err != nil {
		t.Fatalf("UpdateStatus() = %v, se esperaba un no-op exitoso", err)
	}
	if _, found := repo.Get(eventID); found {
		t.Fatal("actualizar un evento inexistente no debe crearlo")
	}
}

func TestUnitOfWork_UpdateStatusDentroDeLaTransaccionActualizaUnEventoYaConfirmado(t *testing.T) {
	logs := NewNotificationLogRepository()
	uow := NewUnitOfWork(logs, NewDeviceTokenRepository())
	eventID := uuid.New()

	if err := logs.Register(context.Background(), &domain.NotificationLog{ID: uuid.New(), EventID: eventID, Status: domain.StatusFailed, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("Register() = %v", err)
	}

	err := uow.Do(context.Background(), func(tx domain.TxRepositories) error {
		return tx.NotificationLogs.UpdateStatus(context.Background(), eventID, domain.StatusSkippedNoToken)
	})
	if err != nil {
		t.Fatalf("Do() = %v", err)
	}

	entry, _ := logs.Get(eventID)
	if entry.Status != domain.StatusSkippedNoToken {
		t.Fatalf("Status = %q, se esperaba %q", entry.Status, domain.StatusSkippedNoToken)
	}
}
