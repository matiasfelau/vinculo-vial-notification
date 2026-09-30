package config

import "testing"

func TestLoad_AplicaValoresPorDefecto(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("MAX_RETRIES", "")
	t.Setenv("FCM_BATCH_SIZE", "")
	t.Setenv("FCM_TIMEOUT_SECONDS", "")
	t.Setenv("RABBITMQ_PREFETCH", "")
	t.Setenv("DB_STATEMENT_TIMEOUT_MS", "")
	t.Setenv("DB_MAX_OPEN_CONNS", "")
	t.Setenv("DB_MAX_IDLE_CONNS", "")
	t.Setenv("DB_CONN_MAX_LIFETIME_MINUTES", "")
	t.Setenv("DB_QUERY_TIMEOUT_SECONDS", "")
	t.Setenv("HTTP_READ_TIMEOUT_SECONDS", "")
	t.Setenv("HTTP_WRITE_TIMEOUT_SECONDS", "")
	t.Setenv("HTTP_IDLE_TIMEOUT_SECONDS", "")
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
	if cfg.FCMTimeoutSeconds != 10 {
		t.Errorf("FCMTimeoutSeconds = %d, se esperaba %d", cfg.FCMTimeoutSeconds, 10)
	}
	if cfg.RabbitMQPrefetch != 10 {
		t.Errorf("RabbitMQPrefetch = %d, se esperaba %d", cfg.RabbitMQPrefetch, 10)
	}
	if cfg.DBStatementTimeoutMS != 5000 {
		t.Errorf("DBStatementTimeoutMS = %d, se esperaba %d", cfg.DBStatementTimeoutMS, 5000)
	}
	if cfg.DBMaxOpenConns != 10 {
		t.Errorf("DBMaxOpenConns = %d, se esperaba %d", cfg.DBMaxOpenConns, 10)
	}
	if cfg.DBMaxIdleConns != 5 {
		t.Errorf("DBMaxIdleConns = %d, se esperaba %d", cfg.DBMaxIdleConns, 5)
	}
	if cfg.DBConnMaxLifetimeMinutes != 30 {
		t.Errorf("DBConnMaxLifetimeMinutes = %d, se esperaba %d", cfg.DBConnMaxLifetimeMinutes, 30)
	}
	if cfg.DBQueryTimeoutSeconds != 7 {
		t.Errorf("DBQueryTimeoutSeconds = %d, se esperaba %d", cfg.DBQueryTimeoutSeconds, 7)
	}
	if cfg.HTTPReadTimeoutSeconds != 5 {
		t.Errorf("HTTPReadTimeoutSeconds = %d, se esperaba %d", cfg.HTTPReadTimeoutSeconds, 5)
	}
	if cfg.HTTPWriteTimeoutSeconds != 10 {
		t.Errorf("HTTPWriteTimeoutSeconds = %d, se esperaba %d", cfg.HTTPWriteTimeoutSeconds, 10)
	}
	if cfg.HTTPIdleTimeoutSeconds != 60 {
		t.Errorf("HTTPIdleTimeoutSeconds = %d, se esperaba %d", cfg.HTTPIdleTimeoutSeconds, 60)
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
	t.Setenv("FCM_TIMEOUT_SECONDS", "20")
	t.Setenv("RABBITMQ_PREFETCH", "50")
	t.Setenv("DB_STATEMENT_TIMEOUT_MS", "3000")
	t.Setenv("DB_MAX_OPEN_CONNS", "25")
	t.Setenv("DB_MAX_IDLE_CONNS", "8")
	t.Setenv("DB_CONN_MAX_LIFETIME_MINUTES", "15")
	t.Setenv("DB_QUERY_TIMEOUT_SECONDS", "4")
	t.Setenv("HTTP_READ_TIMEOUT_SECONDS", "3")
	t.Setenv("HTTP_WRITE_TIMEOUT_SECONDS", "7")
	t.Setenv("HTTP_IDLE_TIMEOUT_SECONDS", "45")

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
	if cfg.FCMTimeoutSeconds != 20 {
		t.Errorf("FCMTimeoutSeconds = %d, se esperaba %d", cfg.FCMTimeoutSeconds, 20)
	}
	if cfg.RabbitMQPrefetch != 50 {
		t.Errorf("RabbitMQPrefetch = %d, se esperaba %d", cfg.RabbitMQPrefetch, 50)
	}
	if cfg.DBStatementTimeoutMS != 3000 {
		t.Errorf("DBStatementTimeoutMS = %d, se esperaba %d", cfg.DBStatementTimeoutMS, 3000)
	}
	if cfg.DBMaxOpenConns != 25 {
		t.Errorf("DBMaxOpenConns = %d, se esperaba %d", cfg.DBMaxOpenConns, 25)
	}
	if cfg.DBMaxIdleConns != 8 {
		t.Errorf("DBMaxIdleConns = %d, se esperaba %d", cfg.DBMaxIdleConns, 8)
	}
	if cfg.DBConnMaxLifetimeMinutes != 15 {
		t.Errorf("DBConnMaxLifetimeMinutes = %d, se esperaba %d", cfg.DBConnMaxLifetimeMinutes, 15)
	}
	if cfg.DBQueryTimeoutSeconds != 4 {
		t.Errorf("DBQueryTimeoutSeconds = %d, se esperaba %d", cfg.DBQueryTimeoutSeconds, 4)
	}
	if cfg.HTTPReadTimeoutSeconds != 3 {
		t.Errorf("HTTPReadTimeoutSeconds = %d, se esperaba %d", cfg.HTTPReadTimeoutSeconds, 3)
	}
	if cfg.HTTPWriteTimeoutSeconds != 7 {
		t.Errorf("HTTPWriteTimeoutSeconds = %d, se esperaba %d", cfg.HTTPWriteTimeoutSeconds, 7)
	}
	if cfg.HTTPIdleTimeoutSeconds != 45 {
		t.Errorf("HTTPIdleTimeoutSeconds = %d, se esperaba %d", cfg.HTTPIdleTimeoutSeconds, 45)
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

func TestLoad_ErrorSiFCMTimeoutSecondsNoEsNumerico(t *testing.T) {
	t.Setenv("FCM_TIMEOUT_SECONDS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante FCM_TIMEOUT_SECONDS invalido")
	}
}

func TestLoad_ErrorSiRabbitMQPrefetchNoEsNumerico(t *testing.T) {
	t.Setenv("RABBITMQ_PREFETCH", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante RABBITMQ_PREFETCH invalido")
	}
}

func TestLoad_ErrorSiDBStatementTimeoutMSNoEsNumerico(t *testing.T) {
	t.Setenv("DB_STATEMENT_TIMEOUT_MS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante DB_STATEMENT_TIMEOUT_MS invalido")
	}
}

func TestLoad_ErrorSiDBMaxOpenConnsNoEsNumerico(t *testing.T) {
	t.Setenv("DB_MAX_OPEN_CONNS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante DB_MAX_OPEN_CONNS invalido")
	}
}

func TestLoad_ErrorSiDBMaxIdleConnsNoEsNumerico(t *testing.T) {
	t.Setenv("DB_MAX_IDLE_CONNS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante DB_MAX_IDLE_CONNS invalido")
	}
}

func TestLoad_ErrorSiDBConnMaxLifetimeMinutesNoEsNumerico(t *testing.T) {
	t.Setenv("DB_CONN_MAX_LIFETIME_MINUTES", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante DB_CONN_MAX_LIFETIME_MINUTES invalido")
	}
}

func TestLoad_ErrorSiDBQueryTimeoutSecondsNoEsNumerico(t *testing.T) {
	t.Setenv("DB_QUERY_TIMEOUT_SECONDS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante DB_QUERY_TIMEOUT_SECONDS invalido")
	}
}

func TestLoad_ErrorSiHTTPReadTimeoutSecondsNoEsNumerico(t *testing.T) {
	t.Setenv("HTTP_READ_TIMEOUT_SECONDS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante HTTP_READ_TIMEOUT_SECONDS invalido")
	}
}

func TestLoad_ErrorSiHTTPWriteTimeoutSecondsNoEsNumerico(t *testing.T) {
	t.Setenv("HTTP_WRITE_TIMEOUT_SECONDS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante HTTP_WRITE_TIMEOUT_SECONDS invalido")
	}
}

func TestLoad_ErrorSiHTTPIdleTimeoutSecondsNoEsNumerico(t *testing.T) {
	t.Setenv("HTTP_IDLE_TIMEOUT_SECONDS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("Load() debia devolver un error ante HTTP_IDLE_TIMEOUT_SECONDS invalido")
	}
}
