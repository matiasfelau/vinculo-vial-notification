package dispatch

import (
	"context"
	"errors"
	"log"
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
//
// Idempotencia: el event_id se registra y la push se despacha dentro
// de una misma transaccion que solo se confirma si FCM respondio. Si
// FCM falla, la transaccion se revierte (el event_id queda sin
// registrar) y el error vuelve al consumer, que hace NACK: el
// reintento no se descarta como duplicado y vuelve a intentar el
// envio. Una entrega duplicada concurrente espera en la clave unica
// del event_id y se descarta cuando la primera confirma.
type DispatchAlertService struct {
	unitOfWork   domain.UnitOfWork
	deviceTokens domain.DeviceTokenRepository
	dispatcher   domain.Dispatcher
}

// NewDispatchAlertService construye el servicio. deviceTokens se usa
// fuera de la transaccion, para la limpieza de tokens invalidos.
func NewDispatchAlertService(
	unitOfWork domain.UnitOfWork,
	deviceTokens domain.DeviceTokenRepository,
	dispatcher domain.Dispatcher,
) *DispatchAlertService {
	return &DispatchAlertService{
		unitOfWork:   unitOfWork,
		deviceTokens: deviceTokens,
		dispatcher:   dispatcher,
	}
}

// Execute aplica el flujo documentado para alert.created. Un error de
// retorno nil no implica que se haya despachado una push: cubre tanto
// el redelivery de un evento ya procesado como la ausencia de tokens
// para los destinatarios, casos que el consumer debe confirmar (ACK)
// igual que un despacho exitoso.
//
// Limitacion conocida: el despacho se particiona en lotes de hasta 500
// tokens. Si un lote falla despues de que otros ya se enviaron, el
// rollback y el reintento reenvian los lotes que ya habian salido.
func (s *DispatchAlertService) Execute(ctx context.Context, event AlertCreatedEvent) error {
	var invalidTokens []string

	err := s.unitOfWork.Do(ctx, func(tx domain.TxRepositories) error {
		logEntry := &domain.NotificationLog{
			ID:        uuid.New(),
			EventID:   event.EventID,
			Status:    domain.StatusSent,
			CreatedAt: time.Now(),
		}

		if err := tx.NotificationLogs.Register(ctx, logEntry); err != nil {
			return err
		}

		tokens, err := tx.DeviceTokens.FindByOwnerIDs(ctx, event.OwnerIDs)
		if err != nil {
			return err
		}

		if len(tokens) == 0 {
			return tx.NotificationLogs.UpdateStatus(ctx, event.EventID, domain.StatusSkippedNoToken)
		}

		fcmTokens := make([]string, len(tokens))
		for i, token := range tokens {
			fcmTokens[i] = token.FCMToken
		}

		outcomes, err := s.dispatcher.Dispatch(ctx, fcmTokens, event.AlertType)
		if err != nil {
			return err
		}

		invalidTokens = collectInvalidTokens(outcomes)
		return nil
	})
	if errors.Is(err, domain.ErrEventAlreadyProcessed) {
		return nil
	}
	if err != nil {
		return err
	}

	s.cleanupInvalidTokens(ctx, invalidTokens)
	return nil
}

func collectInvalidTokens(outcomes []domain.DispatchOutcome) []string {
	var invalidTokens []string
	for _, outcome := range outcomes {
		if outcome.Invalid {
			invalidTokens = append(invalidTokens, outcome.Token)
		}
	}
	return invalidTokens
}

// cleanupInvalidTokens borra los tokens que FCM reporto como
// Unregistered o InvalidArgument (limpieza autonoma). Corre despues de
// confirmar la transaccion y es deliberadamente best-effort: un fallo
// del borrado dentro de la transaccion la dejaria abortada y forzaria
// el rollback de un envio que si tuvo exito, duplicando la push en el
// reintento. La proxima entrega a ese token vuelve a reportarlo.
func (s *DispatchAlertService) cleanupInvalidTokens(ctx context.Context, invalidTokens []string) {
	if len(invalidTokens) == 0 {
		return
	}

	if err := s.deviceTokens.DeleteByTokens(ctx, invalidTokens); err != nil {
		log.Printf("no se pudieron eliminar los tokens invalidos reportados por fcm: %v", err)
	}
}
