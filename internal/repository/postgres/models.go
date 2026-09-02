package postgres

import (
	"time"

	"github.com/google/uuid"
)

// DeviceTokenModel es la proyeccion GORM de domain.DeviceToken. Vive
// exclusivamente en la capa de persistencia: el dominio no conoce estos
// tags.
type DeviceTokenModel struct {
	ID        uuid.UUID `gorm:"type:uuid;primaryKey"`
	OwnerID   uuid.UUID `gorm:"type:uuid;index;not null"`
	FCMToken  string    `gorm:"column:fcm_token;uniqueIndex;not null"`
	UpdatedAt time.Time `gorm:"not null"`
}

func (DeviceTokenModel) TableName() string {
	return "device_tokens"
}

// NotificationLogModel es la proyeccion GORM de domain.NotificationLog.
type NotificationLogModel struct {
	ID          uuid.UUID  `gorm:"type:uuid;primaryKey"`
	EventID     uuid.UUID  `gorm:"column:event_id;uniqueIndex;not null"`
	RecipientID *uuid.UUID `gorm:"column:recipient_id;index"`
	Status      string     `gorm:"not null"`
	CreatedAt   time.Time  `gorm:"not null"`
}

func (NotificationLogModel) TableName() string {
	return "notification_log"
}
