package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// DeviceTokenRepository es la implementacion GORM/Postgres de
// domain.DeviceTokenRepository.
type DeviceTokenRepository struct {
	db           *gorm.DB
	queryTimeout time.Duration
}

// NewDeviceTokenRepository construye el repositorio. queryTimeout
// acota cada consulta individual, independientemente del contexto que
// traiga el llamador (la request HTTP y el ctx de los workers no
// tienen deadline propio). queryTimeout <= 0 desactiva el limite.
func NewDeviceTokenRepository(db *gorm.DB, queryTimeout time.Duration) *DeviceTokenRepository {
	return &DeviceTokenRepository{db: db, queryTimeout: queryTimeout}
}

var _ domain.DeviceTokenRepository = (*DeviceTokenRepository)(nil)

// Upsert inserta el token o, si el fcm_token ya existe, reasigna su
// owner_id y actualiza updated_at. El id y el fcm_token de la fila
// existente se preservan: DoUpdates solo toca las columnas indicadas.
func (r *DeviceTokenRepository) Upsert(ctx context.Context, token *domain.DeviceToken) error {
	ctx, cancel := withQueryTimeout(ctx, r.queryTimeout)
	defer cancel()

	model := DeviceTokenModel{
		ID:        token.ID,
		OwnerID:   token.OwnerID,
		FCMToken:  token.FCMToken,
		UpdatedAt: token.UpdatedAt,
	}

	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "fcm_token"}},
			DoUpdates: clause.AssignmentColumns([]string{"owner_id", "updated_at"}),
		}).
		Create(&model).Error
}

func (r *DeviceTokenRepository) Delete(ctx context.Context, ownerID uuid.UUID, fcmToken string) error {
	ctx, cancel := withQueryTimeout(ctx, r.queryTimeout)
	defer cancel()

	return r.db.WithContext(ctx).
		Where("owner_id = ? AND fcm_token = ?", ownerID, fcmToken).
		Delete(&DeviceTokenModel{}).Error
}

func (r *DeviceTokenRepository) FindByOwnerIDs(ctx context.Context, ownerIDs []uuid.UUID) ([]domain.DeviceToken, error) {
	if len(ownerIDs) == 0 {
		return nil, nil
	}

	ctx, cancel := withQueryTimeout(ctx, r.queryTimeout)
	defer cancel()

	var models []DeviceTokenModel
	if err := r.db.WithContext(ctx).Where("owner_id IN ?", ownerIDs).Find(&models).Error; err != nil {
		return nil, err
	}

	tokens := make([]domain.DeviceToken, 0, len(models))
	for _, m := range models {
		tokens = append(tokens, domain.DeviceToken{
			ID:        m.ID,
			OwnerID:   m.OwnerID,
			FCMToken:  m.FCMToken,
			UpdatedAt: m.UpdatedAt,
		})
	}
	return tokens, nil
}

func (r *DeviceTokenRepository) DeleteByTokens(ctx context.Context, fcmTokens []string) error {
	if len(fcmTokens) == 0 {
		return nil
	}

	ctx, cancel := withQueryTimeout(ctx, r.queryTimeout)
	defer cancel()

	return r.db.WithContext(ctx).Where("fcm_token IN ?", fcmTokens).Delete(&DeviceTokenModel{}).Error
}

func (r *DeviceTokenRepository) DeleteByOwnerID(ctx context.Context, ownerID uuid.UUID) error {
	ctx, cancel := withQueryTimeout(ctx, r.queryTimeout)
	defer cancel()

	return r.db.WithContext(ctx).Where("owner_id = ?", ownerID).Delete(&DeviceTokenModel{}).Error
}
