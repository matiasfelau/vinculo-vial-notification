package tokens

import (
	"context"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// DeleteTokenService da de baja el token FCM de un dispositivo. La
// operacion es idempotente: no distingue entre "no existia" y "no
// pertenecia a este owner", devolviendo exito en ambos casos para no
// habilitar enumeracion de tokens ajenos.
type DeleteTokenService struct {
	repository domain.DeviceTokenRepository
}

func NewDeleteTokenService(repository domain.DeviceTokenRepository) *DeleteTokenService {
	return &DeleteTokenService{repository: repository}
}

func (s *DeleteTokenService) Execute(ctx context.Context, ownerID uuid.UUID, fcmToken string) error {
	return s.repository.Delete(ctx, ownerID, fcmToken)
}
