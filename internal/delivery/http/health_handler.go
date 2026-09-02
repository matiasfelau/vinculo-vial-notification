package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// HealthHandler informa que el proceso esta activo y en condiciones de
// aceptar trafico. No verifica dependencias externas: su unico proposito
// es la sonda de liveness del contenedor.
func HealthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
