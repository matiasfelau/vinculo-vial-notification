package memory

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// NotificationLogRepository implementa domain.NotificationLogRepository
// sobre un mapa protegido por mutex, replicando la semantica de la
// implementacion Postgres: insertar si no existe, de forma atomica, y
// bloquear a quien intente registrar un event_id que otra transaccion
// todavia abierta ya reservo.
type NotificationLogRepository struct {
	mu      sync.Mutex
	logs    map[uuid.UUID]domain.NotificationLog
	pending map[uuid.UUID]chan struct{}
}

func NewNotificationLogRepository() *NotificationLogRepository {
	return &NotificationLogRepository{
		logs:    make(map[uuid.UUID]domain.NotificationLog),
		pending: make(map[uuid.UUID]chan struct{}),
	}
}

var _ domain.NotificationLogRepository = (*NotificationLogRepository)(nil)

// claim reserva eventID para una transaccion. Si otra transaccion
// abierta lo reservo antes, espera a que termine: si confirmo, el
// evento ya fue procesado; si revirtio, la reserva pasa a quien espera.
// Equivale al bloqueo sobre la clave unica que hace Postgres.
func (r *NotificationLogRepository) claim(ctx context.Context, eventID uuid.UUID) (chan struct{}, error) {
	for {
		r.mu.Lock()
		if _, exists := r.logs[eventID]; exists {
			r.mu.Unlock()
			return nil, domain.ErrEventAlreadyProcessed
		}

		wait, busy := r.pending[eventID]
		if !busy {
			done := make(chan struct{})
			r.pending[eventID] = done
			r.mu.Unlock()
			return done, nil
		}
		r.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// settle libera la reserva de eventID. Con entry != nil la confirma
// (el registro pasa a ser visible); con nil la descarta.
func (r *NotificationLogRepository) settle(eventID uuid.UUID, done chan struct{}, entry *domain.NotificationLog) {
	r.mu.Lock()
	if entry != nil {
		r.logs[eventID] = *entry
	}
	delete(r.pending, eventID)
	r.mu.Unlock()

	close(done)
}

// Register inserta el registro en modo autocommit.
func (r *NotificationLogRepository) Register(ctx context.Context, log *domain.NotificationLog) error {
	done, err := r.claim(ctx, log.EventID)
	if err != nil {
		return err
	}

	r.settle(log.EventID, done, log)
	return nil
}

// Get expone el estado actual de un registro confirmado. Pensado para
// inspeccion desde pruebas de otros paquetes; no forma parte de la
// interfaz de dominio.
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

// txNotificationLogs es la vista transaccional del repositorio: los
// registros quedan en staged, invisibles para el resto, hasta commit.
type txNotificationLogs struct {
	parent *NotificationLogRepository
	staged map[uuid.UUID]domain.NotificationLog
	claims map[uuid.UUID]chan struct{}
}

func newTxNotificationLogs(parent *NotificationLogRepository) *txNotificationLogs {
	return &txNotificationLogs{
		parent: parent,
		staged: make(map[uuid.UUID]domain.NotificationLog),
		claims: make(map[uuid.UUID]chan struct{}),
	}
}

var _ domain.NotificationLogRepository = (*txNotificationLogs)(nil)

func (t *txNotificationLogs) Register(ctx context.Context, log *domain.NotificationLog) error {
	done, err := t.parent.claim(ctx, log.EventID)
	if err != nil {
		return err
	}

	t.staged[log.EventID] = *log
	t.claims[log.EventID] = done
	return nil
}

func (t *txNotificationLogs) UpdateStatus(ctx context.Context, eventID uuid.UUID, status domain.NotificationStatus) error {
	if entry, ok := t.staged[eventID]; ok {
		entry.Status = status
		t.staged[eventID] = entry
		return nil
	}
	return t.parent.UpdateStatus(ctx, eventID, status)
}

func (t *txNotificationLogs) commit() {
	for eventID, done := range t.claims {
		entry := t.staged[eventID]
		t.parent.settle(eventID, done, &entry)
	}
	t.claims = nil
}

func (t *txNotificationLogs) rollback() {
	for eventID, done := range t.claims {
		t.parent.settle(eventID, done, nil)
	}
	t.claims = nil
}
