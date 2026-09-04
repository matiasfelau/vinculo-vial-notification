package memory

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// NotificationLogRepository implementa domain.NotificationLogRepository
// sobre un mapa protegido por mutex, replicando la semantica atomica
// de "insertar si no existe" de la implementacion Postgres.
type NotificationLogRepository struct {
	mu   sync.Mutex
	logs map[uuid.UUID]domain.NotificationLog
}

func NewNotificationLogRepository() *NotificationLogRepository {
	return &NotificationLogRepository{logs: make(map[uuid.UUID]domain.NotificationLog)}
}

var _ domain.NotificationLogRepository = (*NotificationLogRepository)(nil)

func (r *NotificationLogRepository) Register(_ context.Context, log *domain.NotificationLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.logs[log.EventID]; exists {
		return domain.ErrEventAlreadyProcessed
	}

	r.logs[log.EventID] = *log
	return nil
}

// Get expone el estado actual de un registro. Pensado para inspeccion
// desde pruebas de otros paquetes; no forma parte de la interfaz de
// dominio.
func (r *NotificationLogRepository) Get(eventID uuid.UUID) (domain.NotificationLog, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.logs[eventID]
	return entry, ok
}

func (r *NotificationLogRepository) UpdateStatus(_ context.Context, eventID uuid.UUID, status domain.NotificationStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, exists := r.logs[eventID]
	if !exists {
		return nil
	}

	entry.Status = status
	r.logs[eventID] = entry
	return nil
}
