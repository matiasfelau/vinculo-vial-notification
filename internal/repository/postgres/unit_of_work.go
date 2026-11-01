package postgres

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// UnitOfWork es la implementacion GORM/Postgres de domain.UnitOfWork.
//
// La espera ante un event_id en vuelo la resuelve el propio motor: un
// INSERT ... ON CONFLICT DO NOTHING sobre una clave unica que otra
// transaccion abierta ya inserto se bloquea hasta que esa transaccion
// termina. Si confirma, el INSERT no afecta filas (duplicado); si
// revierte, el INSERT procede.
type UnitOfWork struct {
	db           *gorm.DB
	queryTimeout time.Duration
}

func NewUnitOfWork(db *gorm.DB, queryTimeout time.Duration) *UnitOfWork {
	return &UnitOfWork{db: db, queryTimeout: queryTimeout}
}

var _ domain.UnitOfWork = (*UnitOfWork)(nil)

func (u *UnitOfWork) Do(ctx context.Context, fn func(tx domain.TxRepositories) error) error {
	return u.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(domain.TxRepositories{
			NotificationLogs: NewNotificationLogRepository(tx, u.queryTimeout),
			DeviceTokens:     NewDeviceTokenRepository(tx, u.queryTimeout),
		})
	})
}
