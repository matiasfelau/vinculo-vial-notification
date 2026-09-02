package postgres

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Connect abre la conexion a la base de datos propia del servicio.
func Connect(dsn string) (*gorm.DB, error) {
	return gorm.Open(postgres.Open(dsn), &gorm.Config{
		TranslateError: true,
	})
}

// AutoMigrate crea o actualiza el esquema de device_tokens y
// notification_log. Se ejecuta al inicializar el repositorio; este
// servicio no gestiona migraciones versionadas independientes.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&DeviceTokenModel{}, &NotificationLogModel{})
}
