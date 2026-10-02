package sessions

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// SessionsRevokedEvent es la porcion del Event Envelope de
// user.sessions.revoked que este servicio necesita. OwnerID proviene
// del actor del envelope: es el usuario cuyas sesiones fueron
// revocadas, no un metadato de auditoria en este flujo.
type SessionsRevokedEvent struct {
	EventID uuid.UUID
	OwnerID uuid.UUID
}

// RevokeSessionsService borra todos los device_tokens de un usuario
// ante la revocacion de sus sesiones. Cubre la Defensa Activa: ante un
// secuestro de sesion, corta las notificaciones a todos los
// dispositivos del usuario, incluido el del atacante.
type RevokeSessionsService struct {
	unitOfWork domain.UnitOfWork
}

func NewRevokeSessionsService(unitOfWork domain.UnitOfWork) *RevokeSessionsService {
	return &RevokeSessionsService{unitOfWork: unitOfWork}
}

// Execute registra el event_id y borra los tokens en una misma
// transaccion. Si el borrado falla, el registro se revierte: el
// reintento no se descarta como duplicado y vuelve a intentar el
// borrado (dado el rol de este flujo en la defensa activa, el fallo
// debe quedar visible via el ciclo de reintentos y terminar en la DLQ,
// nunca silenciarse). Una entrega duplicada concurrente espera en la
// clave unica y se descarta cuando la primera confirma.
func (s *RevokeSessionsService) Execute(ctx context.Context, event SessionsRevokedEvent) error {
	err := s.unitOfWork.Do(ctx, func(tx domain.TxRepositories) error {
		logEntry := &domain.NotificationLog{
			ID:          uuid.New(),
			EventID:     event.EventID,
			RecipientID: &event.OwnerID,
			Status:      domain.StatusSent,
			CreatedAt:   time.Now(),
		}

		if err := tx.NotificationLogs.Register(ctx, logEntry); err != nil {
			return err
		}

		return tx.DeviceTokens.DeleteByOwnerID(ctx, event.OwnerID)
	})
	if errors.Is(err, domain.ErrEventAlreadyProcessed) {
		return nil
	}
	return err
}
