package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// NotificationLogRepository es la implementacion GORM/Postgres de
// domain.NotificationLogRepository.
type NotificationLogRepository struct {
	db           *gorm.DB
	queryTimeout time.Duration
}

// NewNotificationLogRepository construye el repositorio. Ver el
// comentario de queryTimeout en NewDeviceTokenRepository.
func NewNotificationLogRepository(db *gorm.DB, queryTimeout time.Duration) *NotificationLogRepository {
	return &NotificationLogRepository{db: db, queryTimeout: queryTimeout}
}

var _ domain.NotificationLogRepository = (*NotificationLogRepository)(nil)

// Register inserta el registro con ON CONFLICT DO NOTHING sobre
// event_id: es atomico y portable entre motores, a diferencia de
// interpretar el error de violacion de constraint. RowsAffected en 0
// significa que el evento ya estaba registrado.
func (r *NotificationLogRepository) Register(ctx context.Context, log *domain.NotificationLog) error {
	ctx, cancel := withQueryTimeout(ctx, r.queryTimeout)
	defer cancel()

	model := NotificationLogModel{
		ID:          log.ID,
		EventID:     log.EventID,
		RecipientID: log.RecipientID,
		Status:      string(log.Status),
		CreatedAt:   log.CreatedAt,
	}

	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "event_id"}},
			DoNothing: true,
		}).
		Create(&model)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return domain.ErrEventAlreadyProcessed
	}

	return nil
}

func (r *NotificationLogRepository) UpdateStatus(ctx context.Context, eventID uuid.UUID, status domain.NotificationStatus) error {
	ctx, cancel := withQueryTimeout(ctx, r.queryTimeout)
	defer cancel()

	return r.db.WithContext(ctx).
		Model(&NotificationLogModel{}).
		Where("event_id = ?", eventID).
		Update("status", string(status)).Error
}
