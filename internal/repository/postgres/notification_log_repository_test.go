package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

func TestNotificationLogRepository_RegistraUnEventoNuevo(t *testing.T) {
	db := openTestDB(t)
	repo := NewNotificationLogRepository(db, time.Second)

	eventID := uuid.New()
	log := &domain.NotificationLog{
		ID:        uuid.New(),
		EventID:   eventID,
		Status:    domain.StatusSent,
		CreatedAt: time.Now(),
	}

	if err := repo.Register(context.Background(), log); err != nil {
		t.Fatalf("Register() devolvio un error inesperado: %v", err)
	}
}

func TestNotificationLogRepository_MismoEventIDEntregadoDosVeces_SoloUnDespacho(t *testing.T) {
	db := openTestDB(t)
	repo := NewNotificationLogRepository(db, time.Second)
	ctx := context.Background()

	eventID := uuid.New()
	primero := &domain.NotificationLog{
		ID:        uuid.New(),
		EventID:   eventID,
		Status:    domain.StatusSent,
		CreatedAt: time.Now(),
	}
	if err := repo.Register(ctx, primero); err != nil {
		t.Fatalf("Register() del primer intento devolvio un error inesperado: %v", err)
	}

	segundo := &domain.NotificationLog{
		ID:        uuid.New(),
		EventID:   eventID,
		Status:    domain.StatusSent,
		CreatedAt: time.Now(),
	}
	err := repo.Register(ctx, segundo)
	if !errors.Is(err, domain.ErrEventAlreadyProcessed) {
		t.Fatalf("Register() del redelivery = %v, se esperaba %v", err, domain.ErrEventAlreadyProcessed)
	}

	var count int64
	if err := db.Model(&NotificationLogModel{}).Where("event_id = ?", eventID).Count(&count).Error; err != nil {
		t.Fatalf("no se pudo contar las filas: %v", err)
	}
	if count != 1 {
		t.Fatalf("se esperaba una unica fila para el event_id, se encontraron %d", count)
	}
}

func TestNotificationLogRepository_UpdateStatusActualizaElRegistroExistente(t *testing.T) {
	db := openTestDB(t)
	repo := NewNotificationLogRepository(db, time.Second)
	ctx := context.Background()

	eventID := uuid.New()
	log := &domain.NotificationLog{ID: uuid.New(), EventID: eventID, Status: domain.StatusFailed, CreatedAt: time.Now()}
	if err := repo.Register(ctx, log); err != nil {
		t.Fatalf("Register() devolvio un error inesperado: %v", err)
	}

	if err := repo.UpdateStatus(ctx, eventID, domain.StatusSent); err != nil {
		t.Fatalf("UpdateStatus() devolvio un error inesperado: %v", err)
	}

	var stored NotificationLogModel
	if err := db.Where("event_id = ?", eventID).First(&stored).Error; err != nil {
		t.Fatalf("no se pudo leer el registro actualizado: %v", err)
	}
	if stored.Status != string(domain.StatusSent) {
		t.Fatalf("Status = %q, se esperaba %q", stored.Status, domain.StatusSent)
	}
}

func TestNotificationLogRepository_EventosDistintosSeRegistranPorSeparado(t *testing.T) {
	db := openTestDB(t)
	repo := NewNotificationLogRepository(db, time.Second)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		log := &domain.NotificationLog{
			ID:        uuid.New(),
			EventID:   uuid.New(),
			Status:    domain.StatusSent,
			CreatedAt: time.Now(),
		}
		if err := repo.Register(ctx, log); err != nil {
			t.Fatalf("Register() del evento %d devolvio un error inesperado: %v", i, err)
		}
	}

	var count int64
	if err := db.Model(&NotificationLogModel{}).Count(&count).Error; err != nil {
		t.Fatalf("no se pudo contar las filas: %v", err)
	}
	if count != 3 {
		t.Fatalf("se esperaban 3 filas, se encontraron %d", count)
	}
}
