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

// ReadinessHandler expone el estado de las dependencias vitales del
// servicio. /ready chequea solo la base propia y es lo que consultan los
// healthchecks activos de Kong. Un contenedor que pierde conexion con
// la base se saca del balanceo.
type ReadinessHandler struct {
	db dbPinger
}

func NewReadinessHandler(db dbPinger) *ReadinessHandler {
	return &ReadinessHandler{db: db}
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

	status := http.StatusOK
	label := "ready"
	if !ready {
		status = http.StatusServiceUnavailable
		label = "not_ready"
	}

	c.JSON(status, gin.H{"status": label, "checks": checks})
}
