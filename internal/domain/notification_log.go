package domain

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// NotificationStatus resume el resultado del procesamiento de un evento
// para fines de auditoria interna. Nunca se expone por API.
type NotificationStatus string

const (
	StatusSent           NotificationStatus = "SENT"
	StatusFailed         NotificationStatus = "FAILED"
	StatusSkippedNoToken NotificationStatus = "SKIPPED_NO_TOKEN"
)

// NotificationLog es el registro de idempotencia y auditoria del
// despacho. No contiene el contenido de la alerta ni el actor del
// evento: solo el identificador del evento, un destinatario de
// referencia cuando corresponde y el resultado del procesamiento.
type NotificationLog struct {
	ID          uuid.UUID
	EventID     uuid.UUID
	RecipientID *uuid.UUID
	Status      NotificationStatus
	CreatedAt   time.Time
}

// ErrEventAlreadyProcessed se devuelve cuando el event_id ya existe en
// notification_log. Es la senial de deduplicacion que el worker usa
// para hacer ACK inmediato sin despachar.
var ErrEventAlreadyProcessed = errors.New("el evento ya fue procesado")

// NotificationLogRepository define las operaciones de persistencia sobre
// notification_log.
type NotificationLogRepository interface {
	// Register intenta insertar el registro de forma atomica contra el
	// constraint UNIQUE de event_id. Si el evento ya fue registrado,
	// devuelve ErrEventAlreadyProcessed sin alterar la fila existente.
	Register(ctx context.Context, log *NotificationLog) error
}
