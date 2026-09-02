package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// DeviceToken representa el token FCM de un dispositivo asociado a un
// usuario. Un usuario puede registrar multiples tokens (uno por
// dispositivo); un fcm_token, en cambio, pertenece a un unico usuario
// en un momento dado.
type DeviceToken struct {
	ID        uuid.UUID
	OwnerID   uuid.UUID
	FCMToken  string
	UpdatedAt time.Time
}

// DeviceTokenRepository define las operaciones de persistencia sobre
// device_tokens. La capa de dominio no conoce GORM ni Postgres: solo
// depende de esta interfaz.
type DeviceTokenRepository interface {
	// Upsert crea el token o, si el fcm_token ya existe, reasigna su
	// owner_id. Cubre el caso de un dispositivo que cambia de dueno sin
	// duplicar la fila.
	Upsert(ctx context.Context, token *DeviceToken) error

	// Delete da de baja un token por su valor. No es un error que el
	// token no exista: la operacion es idempotente.
	Delete(ctx context.Context, fcmToken string) error

	// FindByOwnerIDs devuelve, en una unica consulta, los tokens de
	// todos los owner_ids indicados.
	FindByOwnerIDs(ctx context.Context, ownerIDs []uuid.UUID) ([]DeviceToken, error)

	// DeleteByTokens borra en lote los tokens indicados. Lo usa la
	// limpieza autonoma ante respuestas Unregistered o InvalidArgument
	// de FCM.
	DeleteByTokens(ctx context.Context, fcmTokens []string) error

	// DeleteByOwnerID borra todos los tokens de un usuario. Lo usa el
	// worker de revocacion de sesiones.
	DeleteByOwnerID(ctx context.Context, ownerID uuid.UUID) error
}
