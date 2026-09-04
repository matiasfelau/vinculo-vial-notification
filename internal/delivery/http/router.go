package http

import "github.com/gin-gonic/gin"

// NewRouter arma el engine de Gin con las rutas publicas del servicio.
func NewRouter() *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", HealthHandler)

	return router
}

// RegisterTokenRoutes agrega el alta y la baja de tokens FCM. El
// limite diario de altas por usuario lo aplica Kong, no este
// servicio.
func RegisterTokenRoutes(router *gin.Engine, handler *TokenHandler) {
	tokens := router.Group("/api/v1/notifications/tokens")
	tokens.Use(OwnerIDMiddleware())

	tokens.POST("", handler.Register)
	tokens.DELETE("", handler.Delete)
}
