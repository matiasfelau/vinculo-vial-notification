package tokens

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// RegisterTokenService da de alta o actualiza el token FCM de un
// dispositivo para un usuario.
type RegisterTokenService struct {
	repository domain.DeviceTokenRepository
}

func NewRegisterTokenService(repository domain.DeviceTokenRepository) *RegisterTokenService {
	return &RegisterTokenService{repository: repository}
}

func (s *RegisterTokenService) Execute(ctx context.Context, ownerID uuid.UUID, fcmToken string) error {
	token := &domain.DeviceToken{
		ID:        uuid.New(),
		OwnerID:   ownerID,
		FCMToken:  fcmToken,
		UpdatedAt: time.Now(),
	}
	return s.repository.Upsert(ctx, token)
}
