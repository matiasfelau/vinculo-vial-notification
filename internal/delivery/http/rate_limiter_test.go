package http

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDailyRateLimiter_PermiteHastaElLimiteYLuegoBloquea(t *testing.T) {
	limiter := NewDailyRateLimiter()
	owner := uuid.New()

	for i := 0; i < dailyTokenRegistrationLimit; i++ {
		if !limiter.Allow(owner) {
			t.Fatalf("intento %d deberia haberse permitido", i+1)
		}
	}

	if limiter.Allow(owner) {
		t.Fatal("el intento numero 11 deberia haberse bloqueado")
	}
}

func TestDailyRateLimiter_NoAfectaAOtroOwner(t *testing.T) {
	limiter := NewDailyRateLimiter()
	ownerA := uuid.New()
	ownerB := uuid.New()

	for i := 0; i < dailyTokenRegistrationLimit; i++ {
		limiter.Allow(ownerA)
	}
	if limiter.Allow(ownerA) {
		t.Fatal("ownerA deberia estar bloqueado")
	}

	if !limiter.Allow(ownerB) {
		t.Fatal("ownerB no deberia verse afectado por la cuota de ownerA")
	}
}

func TestDailyRateLimiter_ReiniciaAlPasarLaVentana(t *testing.T) {
	limiter := NewDailyRateLimiter()
	owner := uuid.New()

	current := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	limiter.now = func() time.Time { return current }

	for i := 0; i < dailyTokenRegistrationLimit; i++ {
		if !limiter.Allow(owner) {
			t.Fatalf("intento %d deberia haberse permitido", i+1)
		}
	}
	if limiter.Allow(owner) {
		t.Fatal("deberia estar bloqueado dentro de la misma ventana")
	}

	current = current.Add(rateLimitWindow + time.Second)
	if !limiter.Allow(owner) {
		t.Fatal("la cuota deberia haberse reiniciado tras pasar la ventana de 24 horas")
	}
}
