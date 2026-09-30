package http

import (
	"context"
	"errors"
	"net/http"
	"testing"
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

func TestClassifyDependencyError_ErrorInesperado_DevuelveBadGateway(t *testing.T) {
	if got := classifyDependencyError(errors.New("violacion de constraint inesperada")); got != http.StatusBadGateway {
		t.Errorf("classifyDependencyError(err generico) = %d, se esperaba %d", got, http.StatusBadGateway)
	}
}
