package memory

import (
	"context"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// UnitOfWork implementa domain.UnitOfWork en memoria. Lo que se
// revierte es el registro del event_id (el punto que define la
// idempotencia); las operaciones sobre device_tokens se aplican en el
// acto, lo que alcanza para las pruebas porque cada flujo hace como
// maximo una escritura sobre tokens y es la ultima del bloque.
type UnitOfWork struct {
	logs   *NotificationLogRepository
	tokens domain.DeviceTokenRepository
}

func NewUnitOfWork(logs *NotificationLogRepository, tokens domain.DeviceTokenRepository) *UnitOfWork {
	return &UnitOfWork{logs: logs, tokens: tokens}
}

var _ domain.UnitOfWork = (*UnitOfWork)(nil)

func (u *UnitOfWork) Do(_ context.Context, fn func(tx domain.TxRepositories) error) (err error) {
	tx := newTxNotificationLogs(u.logs)

	// Si fn entra en panico, las reservas deben liberarse: de lo
	// contrario cualquier duplicado quedaria esperando para siempre.
	committed := false
	defer func() {
		if !committed {
			tx.rollback()
		}
	}()

	if err = fn(domain.TxRepositories{NotificationLogs: tx, DeviceTokens: u.tokens}); err != nil {
		return err
	}

	tx.commit()
	committed = true
	return nil
}
