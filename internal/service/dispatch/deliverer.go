package dispatch

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// deliverer es el nucleo compartido por los casos de uso de despacho:
// registra el event_id, resuelve los tokens de los destinatarios y envia
// la push dentro de una misma transaccion.
//
// Idempotencia: el event_id se registra y la push se despacha dentro de
// una misma transaccion que solo se confirma si FCM respondio. Si FCM
// falla, la transaccion se revierte (el event_id queda sin registrar) y
// el error vuelve al consumer, que hace NACK: el reintento no se
// descarta como duplicado y vuelve a intentar el envio. Una entrega
// duplicada concurrente espera en la clave unica del event_id y se
// descarta cuando la primera confirma.
type deliverer struct {
	unitOfWork   domain.UnitOfWork
	deviceTokens domain.DeviceTokenRepository
	dispatcher   domain.Dispatcher
}

// deliver envia message a los tokens de recipients bajo la clave de
// idempotencia dedupeID. Un error nil no implica que se haya despachado
// una push: cubre tanto el redelivery de un evento ya procesado como la
// ausencia de tokens para los destinatarios, casos que el consumer debe
// confirmar (ACK) igual que un despacho exitoso.
//
// Limitacion conocida: el despacho se particiona en lotes de hasta 500
// tokens. Si un lote falla despues de que otros ya se enviaron, el
// rollback y el reintento reenvian los lotes que ya habian salido.
func (d *deliverer) deliver(ctx context.Context, dedupeID uuid.UUID, recipients []uuid.UUID, message domain.PushMessage) error {
	var invalidTokens []string

	err := d.unitOfWork.Do(ctx, func(tx domain.TxRepositories) error {
		// RecipientID queda nulo a proposito: el destinatario de un
		// aviso al emisor es el actor del evento, y no se persiste
		// junto al evento que lo origino.
		logEntry := &domain.NotificationLog{
			ID:        uuid.New(),
			EventID:   dedupeID,
			Status:    domain.StatusSent,
			CreatedAt: time.Now(),
		}

		if err := tx.NotificationLogs.Register(ctx, logEntry); err != nil {
			return err
		}

		tokens, err := tx.DeviceTokens.FindByOwnerIDs(ctx, recipients)
		if err != nil {
			return err
		}

		if len(tokens) == 0 {
			return tx.NotificationLogs.UpdateStatus(ctx, dedupeID, domain.StatusSkippedNoToken)
		}

		fcmTokens := make([]string, len(tokens))
		for i, token := range tokens {
			fcmTokens[i] = token.FCMToken
		}

		outcomes, err := d.dispatcher.Dispatch(ctx, fcmTokens, message)
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

	d.cleanupInvalidTokens(ctx, invalidTokens)
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
func (d *deliverer) cleanupInvalidTokens(ctx context.Context, invalidTokens []string) {
	if len(invalidTokens) == 0 {
		return
	}

	if err := d.deviceTokens.DeleteByTokens(ctx, invalidTokens); err != nil {
		log.Printf("no se pudieron eliminar los tokens invalidos reportados por fcm: %v", err)
	}
}
