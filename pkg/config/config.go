package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config concentra la totalidad de las variables de entorno que consume
// el servicio, tanto para la API HTTP como para el worker de consumo
// asincronico. Su forma coincide con el contrato documentado en el
// archivo .env.example.
type Config struct {
	Port                    string
	DatabaseURL             string
	RabbitMQURL             string
	RabbitMQExchange        string
	AlertQueue              string
	SessionsRevokedQueue    string
	MaxRetries              int
	FirebaseCredentialsPath string
	FCMBatchSize            int
	FCMTimeoutSeconds       int
	RabbitMQPrefetch        int

	FCMBreakerFailureThreshold int
	FCMBreakerWindowSeconds    int
	FCMBreakerOpenSeconds      int

	DBStatementTimeoutMS     int
	DBMaxOpenConns           int
	DBMaxIdleConns           int
	DBConnMaxLifetimeMinutes int
	DBQueryTimeoutSeconds    int

	HTTPReadTimeoutSeconds  int
	HTTPWriteTimeoutSeconds int
	HTTPIdleTimeoutSeconds  int
}

// Load construye la configuracion a partir de las variables de entorno
// del proceso, aplicando valores por defecto para aquellas que no
// constituyen un secreto y son estables entre entornos.
func Load() (*Config, error) {
	maxRetries, err := parseIntEnv("MAX_RETRIES", 5)
	if err != nil {
		return nil, err
	}

	fcmBatchSize, err := parseIntEnv("FCM_BATCH_SIZE", 500)
	if err != nil {
		return nil, err
	}

	fcmTimeoutSeconds, err := parseIntEnv("FCM_TIMEOUT_SECONDS", 10)
	if err != nil {
		return nil, err
	}

	rabbitMQPrefetch, err := parseIntEnv("RABBITMQ_PREFETCH", 10)
	if err != nil {
		return nil, err
	}

	fcmBreakerFailureThreshold, err := parseIntEnv("FCM_BREAKER_FAILURE_THRESHOLD", 5)
	if err != nil {
		return nil, err
	}

	fcmBreakerWindowSeconds, err := parseIntEnv("FCM_BREAKER_WINDOW_SECONDS", 60)
	if err != nil {
		return nil, err
	}

	fcmBreakerOpenSeconds, err := parseIntEnv("FCM_BREAKER_OPEN_SECONDS", 30)
	if err != nil {
		return nil, err
	}

	dbStatementTimeoutMS, err := parseIntEnv("DB_STATEMENT_TIMEOUT_MS", 1500)
	if err != nil {
		return nil, err
	}

	dbMaxOpenConns, err := parseIntEnv("DB_MAX_OPEN_CONNS", 10)
	if err != nil {
		return nil, err
	}

	dbMaxIdleConns, err := parseIntEnv("DB_MAX_IDLE_CONNS", 5)
	if err != nil {
		return nil, err
	}

	dbConnMaxLifetimeMinutes, err := parseIntEnv("DB_CONN_MAX_LIFETIME_MINUTES", 30)
	if err != nil {
		return nil, err
	}

	dbQueryTimeoutSeconds, err := parseIntEnv("DB_QUERY_TIMEOUT_SECONDS", 2)
	if err != nil {
		return nil, err
	}

	httpReadTimeoutSeconds, err := parseIntEnv("HTTP_READ_TIMEOUT_SECONDS", 5)
	if err != nil {
		return nil, err
	}

	httpWriteTimeoutSeconds, err := parseIntEnv("HTTP_WRITE_TIMEOUT_SECONDS", 6)
	if err != nil {
		return nil, err
	}

	httpIdleTimeoutSeconds, err := parseIntEnv("HTTP_IDLE_TIMEOUT_SECONDS", 60)
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:                    getEnv("PORT", "8080"),
		DatabaseURL:             os.Getenv("DATABASE_URL"),
		RabbitMQURL:             os.Getenv("RABBITMQ_URL"),
		RabbitMQExchange:        getEnv("RABBITMQ_EXCHANGE", "vvi.topic.exchange"),
		AlertQueue:              getEnv("ALERT_QUEUE", "notification.alert-created.q"),
		SessionsRevokedQueue:    getEnv("SESSIONS_REVOKED_QUEUE", "notification.sessions-revoked.q"),
		MaxRetries:              maxRetries,
		FirebaseCredentialsPath: os.Getenv("FIREBASE_CREDENTIALS_PATH"),
		FCMBatchSize:            fcmBatchSize,
		FCMTimeoutSeconds:       fcmTimeoutSeconds,
		RabbitMQPrefetch:        rabbitMQPrefetch,

		FCMBreakerFailureThreshold: fcmBreakerFailureThreshold,
		FCMBreakerWindowSeconds:    fcmBreakerWindowSeconds,
		FCMBreakerOpenSeconds:      fcmBreakerOpenSeconds,

		DBStatementTimeoutMS:     dbStatementTimeoutMS,
		DBMaxOpenConns:           dbMaxOpenConns,
		DBMaxIdleConns:           dbMaxIdleConns,
		DBConnMaxLifetimeMinutes: dbConnMaxLifetimeMinutes,
		DBQueryTimeoutSeconds:    dbQueryTimeoutSeconds,

		HTTPReadTimeoutSeconds:  httpReadTimeoutSeconds,
		HTTPWriteTimeoutSeconds: httpWriteTimeoutSeconds,
		HTTPIdleTimeoutSeconds:  httpIdleTimeoutSeconds,
	}, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func parseIntEnv(key string, fallback int) (int, error) {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("variable de entorno %s invalida: %w", key, err)
	}

	return parsed, nil
}
