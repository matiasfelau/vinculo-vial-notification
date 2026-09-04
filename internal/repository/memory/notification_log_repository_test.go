package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

func TestNotificationLogRepository_MismoEventIDEntregadoDosVeces_SoloUnDespacho(t *testing.T) {
	repo := NewNotificationLogRepository()
	ctx := context.Background()

	eventID := uuid.New()
	primero := &domain.NotificationLog{ID: uuid.New(), EventID: eventID, Status: domain.StatusSent, CreatedAt: time.Now()}
	if err := repo.Register(ctx, primero); err != nil {
		t.Fatalf("Register() del primer intento devolvio un error inesperado: %v", err)
	}

	segundo := &domain.NotificationLog{ID: uuid.New(), EventID: eventID, Status: domain.StatusSent, CreatedAt: time.Now()}
	err := repo.Register(ctx, segundo)
	if !errors.Is(err, domain.ErrEventAlreadyProcessed) {
		t.Fatalf("Register() del redelivery = %v, se esperaba %v", err, domain.ErrEventAlreadyProcessed)
	}
}

func TestNotificationLogRepository_UpdateStatusActualizaElRegistroExistente(t *testing.T) {
	repo := NewNotificationLogRepository()
	ctx := context.Background()

	eventID := uuid.New()
	log := &domain.NotificationLog{ID: uuid.New(), EventID: eventID, Status: domain.StatusFailed, CreatedAt: time.Now()}
	if err := repo.Register(ctx, log); err != nil {
		t.Fatalf("Register() devolvio un error inesperado: %v", err)
	}

	if err := repo.UpdateStatus(ctx, eventID, domain.StatusSent); err != nil {
		t.Fatalf("UpdateStatus() devolvio un error inesperado: %v", err)
	}

	if repo.logs[eventID].Status != domain.StatusSent {
		t.Fatalf("Status = %q, se esperaba %q", repo.logs[eventID].Status, domain.StatusSent)
	}
}

func TestNotificationLogRepository_EventosDistintosSeRegistranPorSeparado(t *testing.T) {
	repo := NewNotificationLogRepository()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		log := &domain.NotificationLog{ID: uuid.New(), EventID: uuid.New(), Status: domain.StatusSent, CreatedAt: time.Now()}
		if err := repo.Register(ctx, log); err != nil {
			t.Fatalf("Register() del evento %d devolvio un error inesperado: %v", i, err)
		}
	}
}
