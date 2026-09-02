package worker

import (
	"context"
	"testing"
	"time"
)

func TestNextBackoff_DuplicaHastaElTope(t *testing.T) {
	backoff := minReconnectBackoff
	for i := 0; i < 10; i++ {
		backoff = nextBackoff(backoff)
	}
	if backoff != maxReconnectBackoff {
		t.Fatalf("backoff = %v, se esperaba que se estabilizara en el tope %v", backoff, maxReconnectBackoff)
	}
}

func TestNextBackoff_NuncaSuperaElTope(t *testing.T) {
	if got := nextBackoff(maxReconnectBackoff); got != maxReconnectBackoff {
		t.Fatalf("nextBackoff(max) = %v, se esperaba que se mantuviera en %v", got, maxReconnectBackoff)
	}
}

func TestSleepOrDone_DevuelveTrueTrasElTiempo(t *testing.T) {
	ctx := context.Background()
	start := time.Now()

	if !sleepOrDone(ctx, 10*time.Millisecond) {
		t.Fatal("se esperaba true al completar la espera")
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Fatalf("la espera termino demasiado rapido: %v", elapsed)
	}
}

func TestSleepOrDone_DevuelveFalseAnteCancelacion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if sleepOrDone(ctx, time.Minute) {
		t.Fatal("se esperaba false ante un contexto ya cancelado")
	}
}
