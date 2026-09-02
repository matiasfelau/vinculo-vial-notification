// Package memory provee implementaciones en memoria de las interfaces
// de repositorio del dominio. Se usan en las pruebas unitarias de las
// capas de servicio y entrega para evitar depender de una base de
// datos real, tal como exige la estrategia de testing de este
// servicio.
package memory

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// DeviceTokenRepository implementa domain.DeviceTokenRepository sobre
// un mapa protegido por mutex, replicando la semantica de UPSERT y
// borrado con alcance de owner de la implementacion Postgres.
type DeviceTokenRepository struct {
	mu     sync.Mutex
	tokens map[string]domain.DeviceToken
}

func NewDeviceTokenRepository() *DeviceTokenRepository {
	return &DeviceTokenRepository{tokens: make(map[string]domain.DeviceToken)}
}

var _ domain.DeviceTokenRepository = (*DeviceTokenRepository)(nil)

func (r *DeviceTokenRepository) Upsert(_ context.Context, token *domain.DeviceToken) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := token.ID
	if existing, ok := r.tokens[token.FCMToken]; ok {
		id = existing.ID
	}

	r.tokens[token.FCMToken] = domain.DeviceToken{
		ID:        id,
		OwnerID:   token.OwnerID,
		FCMToken:  token.FCMToken,
		UpdatedAt: token.UpdatedAt,
	}
	return nil
}

func (r *DeviceTokenRepository) Delete(_ context.Context, ownerID uuid.UUID, fcmToken string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.tokens[fcmToken]
	if !ok || existing.OwnerID != ownerID {
		return nil
	}
	delete(r.tokens, fcmToken)
	return nil
}

func (r *DeviceTokenRepository) FindByOwnerIDs(_ context.Context, ownerIDs []uuid.UUID) ([]domain.DeviceToken, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	wanted := make(map[uuid.UUID]struct{}, len(ownerIDs))
	for _, id := range ownerIDs {
		wanted[id] = struct{}{}
	}

	var result []domain.DeviceToken
	for _, token := range r.tokens {
		if _, ok := wanted[token.OwnerID]; ok {
			result = append(result, token)
		}
	}
	return result, nil
}

func (r *DeviceTokenRepository) DeleteByTokens(_ context.Context, fcmTokens []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, token := range fcmTokens {
		delete(r.tokens, token)
	}
	return nil
}

func (r *DeviceTokenRepository) DeleteByOwnerID(_ context.Context, ownerID uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for token, dt := range r.tokens {
		if dt.OwnerID == ownerID {
			delete(r.tokens, token)
		}
	}
	return nil
}
