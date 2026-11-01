// Package breaker implementa un circuit breaker generico para
// proteger llamadas a una dependencia externa. No conoce HTTP, AMQP
// ni Firebase: cualquier cliente de salida lo envuelve reportando
// exito o falla segun su propio criterio.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen se devuelve por Allow cuando el circuito esta abierto: la
// llamada al destino ni siquiera debe intentarse. Quien lo recibe debe
// tratarlo como la misma clase de fallo transitorio que un timeout o
// un error de conexion (por ejemplo, en Notification: nack sin
// requeue, para que el ciclo de reintentos de la cola aplique).
var ErrOpen = errors.New("circuito abierto")

// State representa el estado del circuito.
type State int

const (
	// StateClosed: funcionamiento normal, todas las llamadas se
	// permiten.
	StateClosed State = iota
	// StateOpen: la dependencia esta fallando; las llamadas se cortan
	// sin tocar la red hasta que venza OpenDuration.
	StateOpen
	// StateHalfOpen: vencio OpenDuration; se permite exactamente un
	// intento de prueba para decidir si cerrar o reabrir.
	StateHalfOpen
)

// Config configura el comportamiento del breaker. Los tres campos son
// obligatorios: sin valores explicitos, un circuito nunca abre o
// nunca llega a probar si la dependencia se recupero.
type Config struct {
	// FailureThreshold es la cantidad de fallas, dentro de Window, que
	// abren el circuito.
	FailureThreshold int
	// Window es el periodo durante el cual las fallas se acumulan. Una
	// falla fuera de la ventana reinicia el conteo en lugar de sumarse
	// a fallas viejas y ya irrelevantes.
	Window time.Duration
	// OpenDuration es cuanto tiempo permanece abierto el circuito antes
	// de pasar a half-open y permitir un intento de prueba.
	OpenDuration time.Duration
}

// Breaker protege una dependencia externa contando fallas dentro de
// una ventana deslizante. Es seguro para uso concurrente.
type Breaker struct {
	cfg Config
	now func() time.Time

	mu             sync.Mutex
	state          State
	failures       int
	firstFailureAt time.Time
	openedAt       time.Time
}

// New construye un Breaker cerrado.
func New(cfg Config) *Breaker {
	return &Breaker{cfg: cfg, now: time.Now}
}

// Allow indica si una llamada puede intentarse. Debe invocarse antes
// de cada intento; Report informa el resultado despues. Un circuito
// abierto que ya cumplio OpenDuration pasa a half-open y permite
// exactamente el intento que lo consulta: las llamadas concurrentes
// que lleguen mientras ese intento esta en vuelo siguen viendo
// half-open y tambien se permiten, ya que half-open no corta
// llamadas, solo decide la proxima transicion segun su resultado.
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != StateOpen {
		return nil
	}

	if b.now().Sub(b.openedAt) < b.cfg.OpenDuration {
		return ErrOpen
	}

	b.state = StateHalfOpen
	return nil
}

// Report informa el resultado de una llamada permitida por Allow.
// Solo deben reportarse fallas de la dependencia en si (timeouts,
// errores de conexion, 5xx): un rechazo de negocio del upstream (4xx)
// no es una falla de disponibilidad y debe reportarse como exito.
func (b *Breaker) Report(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if success {
		b.state = StateClosed
		b.failures = 0
		return
	}

	// Un fallo durante el intento de prueba en half-open reabre el
	// circuito de inmediato, sin esperar a acumular FailureThreshold
	// de nuevo: la dependencia todavia no se recupero.
	if b.state == StateHalfOpen {
		b.open()
		return
	}

	now := b.now()
	if b.failures == 0 || now.Sub(b.firstFailureAt) > b.cfg.Window {
		b.failures = 1
		b.firstFailureAt = now
	} else {
		b.failures++
	}

	if b.failures >= b.cfg.FailureThreshold {
		b.open()
	}
}

func (b *Breaker) open() {
	b.state = StateOpen
	b.openedAt = b.now()
	b.failures = 0
}

// State devuelve el estado actual. Pensado para tests y
// observabilidad; la decision de permitir o no una llamada se toma
// siempre con Allow, nunca inspeccionando State.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
