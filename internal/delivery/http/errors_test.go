package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// fakeNetError implementa net.Error para probar la clasificacion sin
// depender de una falla de red real. net.Error exige Temporary() pese
// a estar deprecado: sin implementarlo, el doble no satisface la
// interfaz y errors.As lo descartaria en silencio.
type fakeNetError struct{ timeout bool }

func (e fakeNetError) Error() string   { return "fake net error" }
func (e fakeNetError) Timeout() bool   { return e.timeout }
func (e fakeNetError) Temporary() bool { return e.timeout }

func TestClassifyDependencyError_DeadlineExceeded_DevuelveServiceUnavailable(t *testing.T) {
	if got := classifyDependencyError(context.DeadlineExceeded); got != http.StatusServiceUnavailable {
		t.Errorf("classifyDependencyError(DeadlineExceeded) = %d, se esperaba %d", got, http.StatusServiceUnavailable)
	}
}

func TestClassifyDependencyError_ContextoCancelado_DevuelveServiceUnavailable(t *testing.T) {
	if got := classifyDependencyError(context.Canceled); got != http.StatusServiceUnavailable {
		t.Errorf("classifyDependencyError(Canceled) = %d, se esperaba %d", got, http.StatusServiceUnavailable)
	}
}

func TestClassifyDependencyError_ErrorDeRed_DevuelveServiceUnavailable(t *testing.T) {
	if got := classifyDependencyError(fakeNetError{timeout: true}); got != http.StatusServiceUnavailable {
		t.Errorf("classifyDependencyError(net.Error) = %d, se esperaba %d", got, http.StatusServiceUnavailable)
	}
}

func TestClassifyDependencyError_ErrorEnvueltoPreservaLaClasificacion(t *testing.T) {
	wrapped := errors.Join(errors.New("contexto adicional"), context.DeadlineExceeded)

	if got := classifyDependencyError(wrapped); got != http.StatusServiceUnavailable {
		t.Errorf("classifyDependencyError(err envuelto) = %d, se esperaba %d", got, http.StatusServiceUnavailable)
	}
}

func TestClassifyDependencyError_ErrorInesperado_DevuelveInternalServerError(t *testing.T) {
	// Un error que no es ni timeout, ni de red, ni un SQLSTATE de
	// disponibilidad es un defecto propio (consulta invalida, bug),
	// no una falla transitoria de la dependencia: no debe tratarse
	// como reintentable.
	if got := classifyDependencyError(errors.New("violacion de constraint inesperada")); got != http.StatusInternalServerError {
		t.Errorf("classifyDependencyError(err generico) = %d, se esperaba %d", got, http.StatusInternalServerError)
	}
}

func TestClassifyDependencyError_QueryCanceled_DevuelveServiceUnavailable(t *testing.T) {
	// 57014 es el SQLSTATE que Postgres devuelve cuando
	// statement_timeout corta una consulta en curso. No llega como
	// context.DeadlineExceeded (ese es el timeout que impone Go, no
	// Postgres), asi que sin este chequeo explicito caia en el
	// default generico.
	err := &pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"}

	if got := classifyDependencyError(err); got != http.StatusServiceUnavailable {
		t.Errorf("classifyDependencyError(57014) = %d, se esperaba %d", got, http.StatusServiceUnavailable)
	}
}

func TestClassifyDependencyError_ClasesDeDisponibilidad_DevuelvenServiceUnavailable(t *testing.T) {
	casos := []struct {
		nombre string
		codigo string
	}{
		{"connection_exception", "08000"},
		{"connection_failure", "08006"},
		{"insufficient_resources", "53000"},
		{"too_many_connections", "53300"},
		{"admin_shutdown", "57P01"},
		{"cannot_connect_now", "57P03"},
	}

	for _, caso := range casos {
		t.Run(caso.nombre, func(t *testing.T) {
			err := &pgconn.PgError{Code: caso.codigo, Message: caso.nombre}

			if got := classifyDependencyError(err); got != http.StatusServiceUnavailable {
				t.Errorf("classifyDependencyError(%s) = %d, se esperaba %d", caso.codigo, got, http.StatusServiceUnavailable)
			}
		})
	}
}

func TestClassifyDependencyError_ErrorDeConsultaPropio_DevuelveInternalServerError(t *testing.T) {
	casos := []struct {
		nombre string
		codigo string
	}{
		{"syntax_error", "42601"},
		{"unique_violation", "23505"},
		{"undefined_column", "42703"},
		{"invalid_text_representation", "22P02"},
		// operator_intervention generico: la seccion Postgres del
		// contrato solo cuenta 57014 y 57P0x dentro de la clase 57,
		// deliberadamente excluyendo el 57000 generico.
		{"operator_intervention_generico", "57000"},
	}

	for _, caso := range casos {
		t.Run(caso.nombre, func(t *testing.T) {
			err := &pgconn.PgError{Code: caso.codigo, Message: caso.nombre}

			if got := classifyDependencyError(err); got != http.StatusInternalServerError {
				t.Errorf("classifyDependencyError(%s) = %d, se esperaba %d (defecto propio, no reintentable)", caso.codigo, got, http.StatusInternalServerError)
			}
		})
	}
}

func TestClassifyDependencyError_PgErrorEnvuelto_PreservaLaClasificacion(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"}
	wrapped := fmt.Errorf("al ejecutar la consulta: %w", pgErr)

	if got := classifyDependencyError(wrapped); got != http.StatusServiceUnavailable {
		t.Errorf("classifyDependencyError(pgErr envuelto) = %d, se esperaba %d", got, http.StatusServiceUnavailable)
	}
}
