package postgres

import (
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// DeviceTokenRepository es la implementacion GORM/Postgres de
// domain.DeviceTokenRepository.
type DeviceTokenRepository struct {
	db *gorm.DB
}

func NewDeviceTokenRepository(db *gorm.DB) *DeviceTokenRepository {
	return &DeviceTokenRepository{db: db}
}

var _ domain.DeviceTokenRepository = (*DeviceTokenRepository)(nil)

// Upsert inserta el token o, si el fcm_token ya existe, reasigna su
// owner_id y actualiza updated_at. El id y el fcm_token de la fila
// existente se preservan: DoUpdates solo toca las columnas indicadas.
func (r *DeviceTokenRepository) Upsert(ctx context.Context, token *domain.DeviceToken) error {
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
	return r.db.WithContext(ctx).
		Where("owner_id = ? AND fcm_token = ?", ownerID, fcmToken).
		Delete(&DeviceTokenModel{}).Error
}

func (r *DeviceTokenRepository) FindByOwnerIDs(ctx context.Context, ownerIDs []uuid.UUID) ([]domain.DeviceToken, error) {
	if len(ownerIDs) == 0 {
		return nil, nil
	}

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
	return r.db.WithContext(ctx).Where("fcm_token IN ?", fcmTokens).Delete(&DeviceTokenModel{}).Error
}

func (r *DeviceTokenRepository) DeleteByOwnerID(ctx context.Context, ownerID uuid.UUID) error {
	return r.db.WithContext(ctx).Where("owner_id = ?", ownerID).Delete(&DeviceTokenModel{}).Error
}
