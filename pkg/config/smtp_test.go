package config

import (
	"strings"
	"testing"
	"time"
)

// clearSMTPEnv deja sin definir todas las variables del canal de email.
func clearSMTPEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"APP_ENV", "EMAIL_REQUESTED_QUEUE",
		"SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_FROM",
		"SMTP_STARTTLS", "SMTP_TIMEOUT",
		"SMTP_BREAKER_FAILURE_THRESHOLD", "SMTP_BREAKER_WINDOW_SECONDS", "SMTP_BREAKER_OPEN_SECONDS",
	} {
		t.Setenv(key, "")
	}
}

func TestLoad_SMTPAplicaLosValoresLocalesPorDefecto(t *testing.T) {
	clearSMTPEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() devolvio un error inesperado: %v", err)
	}

	smtp := cfg.SMTP
	if smtp.Host != "mailpit" || smtp.Port != 1025 {
		t.Errorf("Host/Port = %q/%d, se esperaba mailpit/1025", smtp.Host, smtp.Port)
	}
	if smtp.Username != "" || smtp.Password != "" {
		t.Errorf("Username/Password = %q/%q, se esperaban vacios en local", smtp.Username, smtp.Password)
	}
	if smtp.From != "Vinculo Vial <no-responder@vinculovial.local>" {
		t.Errorf("From = %q", smtp.From)
	}
	if smtp.StartTLS {
		t.Error("StartTLS deberia ser false en local")
	}
	if smtp.Timeout != 5*time.Second {
		t.Errorf("Timeout = %s, se esperaba 5s", smtp.Timeout)
	}
	if smtp.BreakerFailureThreshold != 5 || smtp.BreakerWindowSeconds != 60 || smtp.BreakerOpenSeconds != 30 {
		t.Errorf("breaker = %d/%d/%d, se esperaba 5/60/30 (el mismo criterio que FCM)", smtp.BreakerFailureThreshold, smtp.BreakerWindowSeconds, smtp.BreakerOpenSeconds)
	}
	if cfg.EmailRequestedQueue != "notification.email-requested.q" {
		t.Errorf("EmailRequestedQueue = %q", cfg.EmailRequestedQueue)
	}
}

func TestLoad_SMTPRespetaVariablesExplicitas(t *testing.T) {
	clearSMTPEnv(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("EMAIL_REQUESTED_QUEUE", "otra.cola.q")
	t.Setenv("SMTP_HOST", "email-smtp.us-east-1.amazonaws.com")
	t.Setenv("SMTP_PORT", "587")
	t.Setenv("SMTP_USERNAME", "usuario")
	t.Setenv("SMTP_PASSWORD", "clave")
	t.Setenv("SMTP_FROM", "no-responder@vinculovial.com")
	t.Setenv("SMTP_STARTTLS", "true")
	t.Setenv("SMTP_TIMEOUT", "3s")
	t.Setenv("SMTP_BREAKER_FAILURE_THRESHOLD", "7")
	t.Setenv("SMTP_BREAKER_WINDOW_SECONDS", "90")
	t.Setenv("SMTP_BREAKER_OPEN_SECONDS", "45")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() devolvio un error inesperado: %v", err)
	}

	smtp := cfg.SMTP
	if smtp.Host != "email-smtp.us-east-1.amazonaws.com" || smtp.Port != 587 {
		t.Errorf("Host/Port = %q/%d", smtp.Host, smtp.Port)
	}
	if smtp.Username != "usuario" || smtp.Password != "clave" {
		t.Error("Username/Password no se leyeron del entorno")
	}
	if smtp.From != "no-responder@vinculovial.com" || !smtp.StartTLS || smtp.Timeout != 3*time.Second {
		t.Errorf("From/StartTLS/Timeout = %q/%v/%s", smtp.From, smtp.StartTLS, smtp.Timeout)
	}
	if smtp.BreakerFailureThreshold != 7 || smtp.BreakerWindowSeconds != 90 || smtp.BreakerOpenSeconds != 45 {
		t.Errorf("breaker = %d/%d/%d", smtp.BreakerFailureThreshold, smtp.BreakerWindowSeconds, smtp.BreakerOpenSeconds)
	}
	if cfg.AppEnv != "production" || cfg.EmailRequestedQueue != "otra.cola.q" {
		t.Errorf("AppEnv/EmailRequestedQueue = %q/%q", cfg.AppEnv, cfg.EmailRequestedQueue)
	}
}

func TestLoad_ProduccionExigeSMTPStartTLS(t *testing.T) {
	for _, appEnv := range []string{"production", "PRODUCTION", " Production "} {
		for _, startTLS := range []string{"", "false"} {
			t.Run(appEnv+"/starttls="+startTLS, func(t *testing.T) {
				clearSMTPEnv(t)
				t.Setenv("APP_ENV", appEnv)
				t.Setenv("SMTP_STARTTLS", startTLS)

				_, err := Load()

				if err == nil || !strings.Contains(err.Error(), "SMTP_STARTTLS") {
					t.Fatalf("error = %v, se esperaba un rechazo por SMTP_STARTTLS", err)
				}
			})
		}
	}
}

func TestLoad_FueraDeProduccionPermiteSMTPSinStartTLS(t *testing.T) {
	for _, appEnv := range []string{"", "development", "local"} {
		t.Run("APP_ENV="+appEnv, func(t *testing.T) {
			clearSMTPEnv(t)
			t.Setenv("APP_ENV", appEnv)

			if _, err := Load(); err != nil {
				t.Fatalf("Load() devolvio un error inesperado: %v", err)
			}
		})
	}
}

func TestLoad_SMTPRechazaValoresInvalidos(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
		extra map[string]string
		want  string
	}{
		{"puerto no numerico", "SMTP_PORT", "abc", nil, "SMTP_PORT"},
		{"puerto cero", "SMTP_PORT", "0", nil, "SMTP_PORT"},
		{"puerto fuera de rango", "SMTP_PORT", "70000", nil, "SMTP_PORT"},
		{"starttls no booleano", "SMTP_STARTTLS", "quizas", nil, "SMTP_STARTTLS"},
		{"timeout sin unidad", "SMTP_TIMEOUT", "5", nil, "SMTP_TIMEOUT"},
		{"timeout cero", "SMTP_TIMEOUT", "0s", nil, "SMTP_TIMEOUT"},
		{"timeout negativo", "SMTP_TIMEOUT", "-1s", nil, "SMTP_TIMEOUT"},
		{"umbral del breaker no numerico", "SMTP_BREAKER_FAILURE_THRESHOLD", "x", nil, "SMTP_BREAKER_FAILURE_THRESHOLD"},
		{"ventana del breaker no numerica", "SMTP_BREAKER_WINDOW_SECONDS", "x", nil, "SMTP_BREAKER_WINDOW_SECONDS"},
		{"apertura del breaker no numerica", "SMTP_BREAKER_OPEN_SECONDS", "x", nil, "SMTP_BREAKER_OPEN_SECONDS"},
		{"remitente invalido", "SMTP_FROM", "no-es-una-direccion", nil, "SMTP_FROM"},
		{"usuario sin clave", "SMTP_USERNAME", "usuario", nil, "SMTP_USERNAME"},
		{"clave sin usuario", "SMTP_PASSWORD", "clave", nil, "SMTP_USERNAME"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearSMTPEnv(t)
			t.Setenv(tc.key, tc.value)

			_, err := Load()

			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, se esperaba un rechazo que mencione %s", err, tc.want)
			}
			// Los secretos nunca viajan en el mensaje de error.
			if strings.Contains(err.Error(), "clave") && tc.key == "SMTP_PASSWORD" {
				t.Errorf("el error no deberia reproducir la clave: %v", err)
			}
		})
	}
}
