package config

import "testing"

func TestLoad_AplicaValoresPorDefecto(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("MAX_RETRIES", "")
	t.Setenv("FCM_BATCH_SIZE", "")
	t.Setenv("RABBITMQ_EXCHANGE", "")
	t.Setenv("ALERT_QUEUE", "")
	t.Setenv("SESSIONS_REVOKED_QUEUE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() devolvio un error inesperado: %v", err)
	}

	if cfg.Port != "8080" {
		t.Errorf("Port = %q, se esperaba %q", cfg.Port, "8080")
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, se esperaba %d", cfg.MaxRetries, 5)
	}
	if cfg.FCMBatchSize != 500 {
		t.Errorf("FCMBatchSize = %d, se esperaba %d", cfg.FCMBatchSize, 500)
	}
	if cfg.RabbitMQExchange != "vvi.topic.exchange" {
		t.Errorf("RabbitMQExchange = %q, se esperaba %q", cfg.RabbitMQExchange, "vvi.topic.exchange")
	}
	if cfg.AlertQueue != "notification.alert-created.q" {
		t.Errorf("AlertQueue = %q, se esperaba %q", cfg.AlertQueue, "notification.alert-created.q")
	}
}

func TestLoad_RespetaVariablesExplicitas(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("DATABASE_URL", "postgres://user:pass@host:5432/db")
	t.Setenv("MAX_RETRIES", "3")
	t.Setenv("FCM_BATCH_SIZE", "250")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() devolvio un error inesperado: %v", err)
	}

	if cfg.Port != "9090" {
		t.Errorf("Port = %q, se esperaba %q", cfg.Port, "9090")
	}
	if cfg.DatabaseURL != "postgres://user:pass@host:5432/db" {
		t.Errorf("DatabaseURL = %q, no coincide con el valor explicito", cfg.DatabaseURL)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, se esperaba %d", cfg.MaxRetries, 3)
	}
	if cfg.FCMBatchSize != 250 {
		t.Errorf("FCMBatchSize = %d, se esperaba %d", cfg.FCMBatchSize, 250)
	}
}

func TestLoad_ErrorSiMaxRetriesNoEsNumerico(t *testing.T) {
	t.Setenv("MAX_RETRIES", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante MAX_RETRIES invalido")
	}
}

func TestLoad_ErrorSiFCMBatchSizeNoEsNumerico(t *testing.T) {
	t.Setenv("FCM_BATCH_SIZE", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante FCM_BATCH_SIZE invalido")
	}
}
