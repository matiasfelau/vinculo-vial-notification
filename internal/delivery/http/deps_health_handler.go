package http

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ConnectionChecker informa si una dependencia esta conectada en este
// momento. Los workers de RabbitMQ lo implementan a traves de su
// estado de conexion actual (ver worker.ConnectionStatus).
type ConnectionChecker interface {
	Connected() bool
}

// DepsHealthHandler expone el estado real de todas las dependencias del
// servicio (base de datos y brokers). Su falla se contiene por ruta y su
// estado no afecta el balanceo de Kong.
type DepsHealthHandler struct {
	db      dbPinger
	brokers map[string]ConnectionChecker
}

func NewDepsHealthHandler(db dbPinger, brokers map[string]ConnectionChecker) *DepsHealthHandler {
	return &DepsHealthHandler{db: db, brokers: brokers}
}

func (h *DepsHealthHandler) Handle(c *gin.Context) {
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
