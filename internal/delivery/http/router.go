package http

import "github.com/gin-gonic/gin"

// NewRouter arma el engine de Gin con las rutas publicas del servicio.
func NewRouter() *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", HealthHandler)

	return router
}

// RegisterTokenRoutes agrega el alta y la baja de tokens FCM. El POST
// esta sujeto al limite diario por usuario; el DELETE no se limita,
// dado que bloquear una baja deja al dispositivo recibiendo
// notificaciones que ya no le corresponden.
func RegisterTokenRoutes(router *gin.Engine, handler *TokenHandler, rateLimiter *DailyRateLimiter) {
	tokens := router.Group("/api/v1/notifications/tokens")
	tokens.Use(OwnerIDMiddleware())

	tokens.POST("", rateLimiter.Middleware(), handler.Register)
	tokens.DELETE("", handler.Delete)
}
