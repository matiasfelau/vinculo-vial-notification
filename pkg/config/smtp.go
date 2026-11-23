package config

import (
	"errors"
	"fmt"
	"net/mail"
	"os"
	"strconv"
	"strings"
	"time"
)

// SMTPConfig son los parametros del canal de email (US-13). Los valores
// por defecto son los de desarrollo local contra Mailpit, que no lleva TLS
// ni credenciales; en AWS apuntan al endpoint SMTP de SES.
type SMTPConfig struct {
	Host string
	Port int
	// Username y Password son secretos: solo vienen del entorno y nunca
	// se loguean. Vacios desactivan la autenticacion.
	Username string
	Password string
	From     string
	StartTLS bool
	Timeout  time.Duration

	BreakerFailureThreshold int
	BreakerWindowSeconds    int
	BreakerOpenSeconds      int
}

const (
	defaultSMTPHost = "mailpit"
	defaultSMTPPort = 1025
	defaultSMTPFrom = "Vinculo Vial <no-responder@vinculovial.local>"
)

func loadSMTP(appEnv string) (SMTPConfig, error) {
	port, err := parseIntEnv("SMTP_PORT", defaultSMTPPort)
	if err != nil {
		return SMTPConfig{}, err
	}
	if port < 1 || port > 65535 {
		return SMTPConfig{}, errors.New("variable de entorno SMTP_PORT fuera de rango (1-65535)")
	}

	startTLS, err := parseBoolEnv("SMTP_STARTTLS", false)
	if err != nil {
		return SMTPConfig{}, err
	}

	timeout, err := parseDurationEnv("SMTP_TIMEOUT", 5*time.Second)
	if err != nil {
		return SMTPConfig{}, err
	}

	failureThreshold, err := parseIntEnv("SMTP_BREAKER_FAILURE_THRESHOLD", 5)
	if err != nil {
		return SMTPConfig{}, err
	}
	windowSeconds, err := parseIntEnv("SMTP_BREAKER_WINDOW_SECONDS", 60)
	if err != nil {
		return SMTPConfig{}, err
	}
	openSeconds, err := parseIntEnv("SMTP_BREAKER_OPEN_SECONDS", 30)
	if err != nil {
		return SMTPConfig{}, err
	}

	cfg := SMTPConfig{
		Host:                    getEnv("SMTP_HOST", defaultSMTPHost),
		Port:                    port,
		Username:                os.Getenv("SMTP_USERNAME"),
		Password:                os.Getenv("SMTP_PASSWORD"),
		From:                    getEnv("SMTP_FROM", defaultSMTPFrom),
		StartTLS:                startTLS,
		Timeout:                 timeout,
		BreakerFailureThreshold: failureThreshold,
		BreakerWindowSeconds:    windowSeconds,
		BreakerOpenSeconds:      openSeconds,
	}

	if _, err := mail.ParseAddress(cfg.From); err != nil {
		return SMTPConfig{}, errors.New("variable de entorno SMTP_FROM no es una direccion valida")
	}
	if (cfg.Username == "") != (cfg.Password == "") {
		return SMTPConfig{}, errors.New("SMTP_USERNAME y SMTP_PASSWORD deben definirse juntos")
	}
	if isProduction(appEnv) && !cfg.StartTLS {
		return SMTPConfig{}, errors.New("con APP_ENV=production, SMTP_STARTTLS debe ser true")
	}

	return cfg, nil
}

// isProduction indica si APP_ENV pide el modo produccion, en el que la
// configuracion rechaza los valores inseguros.
func isProduction(appEnv string) bool {
	return strings.EqualFold(strings.TrimSpace(appEnv), "production")
}

func parseBoolEnv(key string, fallback bool) (bool, error) {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("variable de entorno %s invalida: %w", key, err)
	}

	return parsed, nil
}

func parseDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("variable de entorno %s invalida: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("variable de entorno %s debe ser positiva", key)
	}

	return parsed, nil
}
