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
	notificationLogs domain.NotificationLogRepository
	deviceTokens     domain.DeviceTokenRepository
}

func NewRevokeSessionsService(notificationLogs domain.NotificationLogRepository, deviceTokens domain.DeviceTokenRepository) *RevokeSessionsService {
	return &RevokeSessionsService{notificationLogs: notificationLogs, deviceTokens: deviceTokens}
}

// Execute aplica el mismo tratamiento de idempotencia que
// DispatchAlertService: el INSERT de deduplicacion se hace antes de la
// accion. Un fallo del borrado se propaga (a diferencia de la
// limpieza de tokens invalidos del despacho): dado el rol de este
// flujo en la defensa activa ante un secuestro de sesion, es
// preferible que el fallo quede visible via el ciclo de reintentos y
// termine en la DLQ antes que silenciarlo.
func (s *RevokeSessionsService) Execute(ctx context.Context, event SessionsRevokedEvent) error {
	logEntry := &domain.NotificationLog{
		ID:          uuid.New(),
		EventID:     event.EventID,
		RecipientID: &event.OwnerID,
		Status:      domain.StatusSent,
		CreatedAt:   time.Now(),
	}

	if err := s.notificationLogs.Register(ctx, logEntry); err != nil {
		if errors.Is(err, domain.ErrEventAlreadyProcessed) {
			return nil
		}
		return err
	}

	return s.deviceTokens.DeleteByOwnerID(ctx, event.OwnerID)
}
