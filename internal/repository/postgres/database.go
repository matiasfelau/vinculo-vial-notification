package postgres

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// PoolConfig agrupa los limites de conexion aplicados sobre el *sql.DB
// subyacente y el statement_timeout impuesto del lado del motor. Sin
// ellos, el pool por defecto de database/sql no tiene techo y una
// consulta puede colgarse indefinidamente: una base lenta o caida
// puede acumular conexiones y goroutines sin limite, con el mismo
// riesgo que un worker sin timeout hacia FCM.
type PoolConfig struct {
	// StatementTimeoutMS corta, del lado de Postgres, cualquier
	// consulta que exceda este tiempo. Es un limite duro que no
	// depende de que el codigo Go propague un context con deadline
	// correctamente.
	StatementTimeoutMS int
	MaxOpenConns       int
	MaxIdleConns       int
	ConnMaxLifetime    time.Duration
}

// Connect abre la conexion a la base de datos propia del servicio,
// acotando el statement_timeout y el tamanio del pool de conexiones
// segun pool.
func Connect(dsn string, pool PoolConfig) (*gorm.DB, error) {
	dsnWithTimeout, err := withStatementTimeout(dsn, pool.StatementTimeoutMS)
	if err != nil {
		return nil, fmt.Errorf("dsn invalido: %w", err)
	}

	db, err := gorm.Open(postgres.Open(dsnWithTimeout), &gorm.Config{
		TranslateError: true,
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(pool.MaxOpenConns)
	sqlDB.SetMaxIdleConns(pool.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(pool.ConnMaxLifetime)

	return db, nil
}

// withStatementTimeout agrega (o reemplaza) el parametro
// statement_timeout en la cadena de conexion, en milisegundos.
// Postgres lo acepta como parametro de arranque de la sesion via pgx:
// corta cualquier consulta que se cuelgue del lado del motor, sin
// depender de que quien complete el .env se acuerde de incluirlo a
// mano en DATABASE_URL.
func withStatementTimeout(dsn string, timeoutMS int) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	query.Set("statement_timeout", strconv.Itoa(timeoutMS))
	parsed.RawQuery = query.Encode()

	return parsed.String(), nil
}

// AutoMigrate crea o actualiza el esquema de device_tokens y
// notification_log. Se ejecuta al inicializar el repositorio; este
// servicio no gestiona migraciones versionadas independientes.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&DeviceTokenModel{}, &NotificationLogModel{})
}
