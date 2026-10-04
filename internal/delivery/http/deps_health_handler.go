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
//
// Ante una dependencia caida responde 503 con "status":"degraded": la
// instancia sigue lista para el balanceo (eso lo dice /ready, que solo
// mira la base), pero opera degradada. Por eso no usa "not_ready", que
// es el estado de /ready y significaria que Kong debe sacarla.
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
	healthy := true

	if err := h.db.PingContext(ctx); err != nil {
		checks["database"] = "down"
		healthy = false
	} else {
		checks["database"] = "up"
	}

	for name, checker := range h.brokers {
		if checker.Connected() {
			checks[name] = "up"
		} else {
			checks[name] = "down"
			healthy = false
		}
	}

	status := http.StatusOK
	label := "ready"
	if !healthy {
		status = http.StatusServiceUnavailable
		label = "degraded"
	}

	c.JSON(status, gin.H{"status": label, "checks": checks})
}
