package http

import "github.com/gin-gonic/gin"

// NewRouter arma el engine de Gin con las rutas publicas del servicio.
// Los endpoints de tokens FCM se incorporan aca a medida que se
// implementan (feature/token-endpoints).
func NewRouter() *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", HealthHandler)

	return router
}
