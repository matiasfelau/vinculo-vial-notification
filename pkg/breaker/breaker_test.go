package breaker

import (
	"testing"
	"time"
)

// fakeClock permite controlar el paso del tiempo en los tests sin
// sleeps reales.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(cfg Config) (*Breaker, *fakeClock) {
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b := New(cfg)
	b.now = clock.now
	return b, clock
}

func TestBreaker_EmpiezaCerrado(t *testing.T) {
	b, _ := newTestBreaker(Config{FailureThreshold: 3, Window: time.Minute, OpenDuration: time.Minute})

	if b.State() != StateClosed {
		t.Fatalf("estado inicial = %v, se esperaba StateClosed", b.State())
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("Allow() = %v, se esperaba nil con el circuito cerrado", err)
	}
}

func TestBreaker_AbreAlAlcanzarElUmbralDeFallas(t *testing.T) {
	b, _ := newTestBreaker(Config{FailureThreshold: 3, Window: time.Minute, OpenDuration: time.Minute})

	b.Report(false)
	b.Report(false)
	if b.State() != StateClosed {
		t.Fatalf("estado tras 2 fallas (umbral 3) = %v, se esperaba StateClosed", b.State())
	}

	b.Report(false)
	if b.State() != StateOpen {
		t.Fatalf("estado tras 3 fallas (umbral 3) = %v, se esperaba StateOpen", b.State())
	}
}

func TestBreaker_AllowDevuelveErrOpenConElCircuitoAbierto(t *testing.T) {
	b, _ := newTestBreaker(Config{FailureThreshold: 1, Window: time.Minute, OpenDuration: time.Minute})

	b.Report(false)

	if err := b.Allow(); err != ErrOpen {
		t.Fatalf("Allow() = %v, se esperaba ErrOpen", err)
	}
}

func TestBreaker_UnExitoCierraElCircuitoYReseteaElContador(t *testing.T) {
	b, _ := newTestBreaker(Config{FailureThreshold: 3, Window: time.Minute, OpenDuration: time.Minute})

	b.Report(false)
	b.Report(false)
	b.Report(true)

	if b.State() != StateClosed {
		t.Fatalf("estado tras un exito = %v, se esperaba StateClosed", b.State())
	}

	// El contador debe haberse reseteado: dos fallas mas no deberian
	// alcanzar el umbral de 3.
	b.Report(false)
	b.Report(false)
	if b.State() != StateClosed {
		t.Fatalf("estado tras 2 fallas post-reset = %v, se esperaba StateClosed", b.State())
	}
}

func TestBreaker_FallasFueraDeLaVentanaNoAcumulan(t *testing.T) {
	b, clock := newTestBreaker(Config{FailureThreshold: 2, Window: 10 * time.Second, OpenDuration: time.Minute})

	b.Report(false)
	clock.advance(11 * time.Second)
	b.Report(false)

	if b.State() != StateClosed {
		t.Fatalf("estado tras 2 fallas separadas por mas que Window = %v, se esperaba StateClosed", b.State())
	}
}

func TestBreaker_PasaAHalfOpenTrasOpenDuration(t *testing.T) {
	b, clock := newTestBreaker(Config{FailureThreshold: 1, Window: time.Minute, OpenDuration: 30 * time.Second})

	b.Report(false)
	if err := b.Allow(); err != ErrOpen {
		t.Fatalf("Allow() inmediatamente despues de abrir = %v, se esperaba ErrOpen", err)
	}

	clock.advance(30 * time.Second)

	if err := b.Allow(); err != nil {
		t.Fatalf("Allow() tras OpenDuration = %v, se esperaba nil (half-open permite el intento de prueba)", err)
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("estado tras OpenDuration = %v, se esperaba StateHalfOpen", b.State())
	}
}

func TestBreaker_ExitoEnHalfOpenCierraElCircuito(t *testing.T) {
	b, clock := newTestBreaker(Config{FailureThreshold: 1, Window: time.Minute, OpenDuration: 30 * time.Second})

	b.Report(false)
	clock.advance(30 * time.Second)
	_ = b.Allow() // transiciona a half-open

	b.Report(true)

	if b.State() != StateClosed {
		t.Fatalf("estado tras exito en half-open = %v, se esperaba StateClosed", b.State())
	}
}

func TestBreaker_FalloEnHalfOpenReabreElCircuitoDeInmediato(t *testing.T) {
	b, clock := newTestBreaker(Config{FailureThreshold: 5, Window: time.Minute, OpenDuration: 30 * time.Second})

	b.Report(false)
	b.Report(false)
	b.Report(false)
	b.Report(false)
	b.Report(false) // alcanza el umbral de 5, abre

	clock.advance(30 * time.Second)
	_ = b.Allow() // transiciona a half-open

	b.Report(false) // una sola falla alcanza para reabrir, no 5

	if b.State() != StateOpen {
		t.Fatalf("estado tras fallo en half-open = %v, se esperaba StateOpen", b.State())
	}
	if err := b.Allow(); err != ErrOpen {
		t.Fatalf("Allow() tras reabrir = %v, se esperaba ErrOpen", err)
	}
}
