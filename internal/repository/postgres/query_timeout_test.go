package postgres

import (
	"context"
	"testing"
	"time"
)

func TestWithQueryTimeout_AplicaUnDeadlineCuandoElTimeoutEsPositivo(t *testing.T) {
	ctx, cancel := withQueryTimeout(context.Background(), 5*time.Second)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("se esperaba que el contexto resultante tuviera un deadline")
	}

	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > 5*time.Second {
		t.Errorf("deadline fuera del rango esperado: faltan %v", remaining)
	}
}

func TestWithQueryTimeout_SinTimeoutDevuelveElContextoOriginal(t *testing.T) {
	original := context.Background()

	ctx, cancel := withQueryTimeout(original, 0)
	defer cancel()

	if ctx != original {
		t.Error("se esperaba que se devolviera el mismo contexto cuando timeout <= 0")
	}
	if _, ok := ctx.Deadline(); ok {
		t.Error("no se esperaba un deadline cuando timeout <= 0")
	}
}

func TestWithQueryTimeout_RespetaElDeadlineDelContextoPadreSiEsMasEstricto(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer parentCancel()

	ctx, cancel := withQueryTimeout(parent, time.Hour)
	defer cancel()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("se esperaba que el contexto derivado respetara el deadline mas estricto del padre")
	}
}
