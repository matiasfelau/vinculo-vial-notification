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
