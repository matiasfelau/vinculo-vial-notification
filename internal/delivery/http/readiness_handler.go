package http

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// readinessPingTimeout acota cuanto puede tardar el ping a la base
// dentro de /ready. Una dependencia que no responde rapido a un ping
// tampoco va a servir requests: no tiene sentido que /ready se cuelgue
// esperando.
const readinessPingTimeout = 2 * time.Second

// dbPinger es el subconjunto de *sql.DB que /ready necesita.
type dbPinger interface {
	PingContext(ctx context.Context) error
}

// ConnectionChecker informa si una dependencia esta conectada en este
// momento. Los workers de RabbitMQ lo implementan a traves de su
// estado de conexion actual (ver worker.ConnectionStatus).
type ConnectionChecker interface {
	Connected() bool
}

// ReadinessHandler expone el estado real de las dependencias del
// servicio, a diferencia de /health (liveness pura, sin dependencias).
// Kong lo usa como healthcheck pasivo para sacar del balanceo una
// instancia que sigue viva pero no puede cumplir su funcion: un
// contenedor que responde HTTP pero perdio la conexion con la base o
// con el broker es el peor estado posible, porque parece sano y no
// notifica a nadie.
type ReadinessHandler struct {
	db      dbPinger
	brokers map[string]ConnectionChecker
}

func NewReadinessHandler(db dbPinger, brokers map[string]ConnectionChecker) *ReadinessHandler {
	return &ReadinessHandler{db: db, brokers: brokers}
}

func (h *ReadinessHandler) Handle(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), readinessPingTimeout)
	defer cancel()

	checks := gin.H{}
	ready := true

	if err := h.db.PingContext(ctx); err != nil {
		checks["database"] = "down"
		ready = false
	} else {
		checks["database"] = "up"
	}

	for name, checker := range h.brokers {
		if checker.Connected() {
			checks[name] = "up"
		} else {
			checks[name] = "down"
			ready = false
		}
	}

	status := http.StatusOK
	label := "ready"
	if !ready {
		status = http.StatusServiceUnavailable
		label = "not_ready"
	}

	c.JSON(status, gin.H{"status": label, "checks": checks})
}
