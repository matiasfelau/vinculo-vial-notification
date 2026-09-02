package dispatch

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// AlertCreatedEvent es la porcion del Event Envelope de alert.created
// que este servicio necesita, ya extraida y validada por el consumer.
// El actor del evento no forma parte de esta estructura a proposito:
// nunca debe llegar hasta el despacho.
type AlertCreatedEvent struct {
	EventID   uuid.UUID
	OwnerIDs  []uuid.UUID
	AlertType string
}

// DispatchAlertService procesa un evento alert.created: deduplica,
// resuelve los tokens de los destinatarios y despacha la notificacion
// push generica correspondiente.
type DispatchAlertService struct {
	notificationLogs domain.NotificationLogRepository
	deviceTokens     domain.DeviceTokenRepository
	dispatcher       domain.Dispatcher
}

func NewDispatchAlertService(
	notificationLogs domain.NotificationLogRepository,
	deviceTokens domain.DeviceTokenRepository,
	dispatcher domain.Dispatcher,
) *DispatchAlertService {
	return &DispatchAlertService{
		notificationLogs: notificationLogs,
		deviceTokens:     deviceTokens,
		dispatcher:       dispatcher,
	}
}

// Execute aplica el flujo documentado para alert.created. Un error de
// retorno nil no implica que se haya despachado una push: cubre tanto
// el redelivery de un evento ya procesado como la ausencia de tokens
// para los destinatarios, casos que el consumer debe confirmar (ACK)
// igual que un despacho exitoso.
func (s *DispatchAlertService) Execute(ctx context.Context, event AlertCreatedEvent) error {
	logEntry := &domain.NotificationLog{
		ID:        uuid.New(),
		EventID:   event.EventID,
		Status:    domain.StatusFailed,
		CreatedAt: time.Now(),
	}

	if err := s.notificationLogs.Register(ctx, logEntry); err != nil {
		if errors.Is(err, domain.ErrEventAlreadyProcessed) {
			return nil
		}
		return err
	}

	tokens, err := s.deviceTokens.FindByOwnerIDs(ctx, event.OwnerIDs)
	if err != nil {
		return err
	}

	if len(tokens) == 0 {
		return s.notificationLogs.UpdateStatus(ctx, event.EventID, domain.StatusSkippedNoToken)
	}

	fcmTokens := make([]string, len(tokens))
	for i, token := range tokens {
		fcmTokens[i] = token.FCMToken
	}

	if _, err := s.dispatcher.Dispatch(ctx, fcmTokens, event.AlertType); err != nil {
		return err
	}

	return s.notificationLogs.UpdateStatus(ctx, event.EventID, domain.StatusSent)
}
