package postgres

import (
	"net/url"
	"testing"
)

func TestWithStatementTimeout_AgregaElParametroAlDSN(t *testing.T) {
	dsn, err := withStatementTimeout("postgres://user:pass@host:5432/db?sslmode=disable", 5000)
	if err != nil {
		t.Fatalf("withStatementTimeout() devolvio un error inesperado: %v", err)
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("el dsn resultante no es una URL valida: %v", err)
	}

	if got := parsed.Query().Get("statement_timeout"); got != "5000" {
		t.Errorf("statement_timeout = %q, se esperaba %q", got, "5000")
	}
	if got := parsed.Query().Get("sslmode"); got != "disable" {
		t.Errorf("sslmode = %q, se esperaba que se preservara %q", got, "disable")
	}
}

func TestWithStatementTimeout_ReemplazaUnValorPrevio(t *testing.T) {
	dsn, err := withStatementTimeout("postgres://user:pass@host:5432/db?statement_timeout=1", 9000)
	if err != nil {
		t.Fatalf("withStatementTimeout() devolvio un error inesperado: %v", err)
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("el dsn resultante no es una URL valida: %v", err)
	}

	if got := parsed.Query().Get("statement_timeout"); got != "9000" {
		t.Errorf("statement_timeout = %q, se esperaba que se sobreescribiera a %q", got, "9000")
	}
}

func TestWithStatementTimeout_ErrorAnteDSNInvalido(t *testing.T) {
	if _, err := withStatementTimeout("://dsn-invalido", 5000); err == nil {
		t.Fatal("se esperaba un error ante un DSN invalido")
	}
}
