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

// RegisterReadinessRoute agrega el healthcheck de dependencias vitales. A
// diferencia de /health, /ready refleja el estado real de la base de datos:
// Kong lo usa para sacar del balanceo una instancia que sigue viva pero no
// puede cumplir su funcion principal.
func RegisterReadinessRoute(router *gin.Engine, handler *ReadinessHandler) {
	router.GET("/ready", handler.Handle)
}

// RegisterDepsHealthRoute agrega el healthcheck completo de dependencias.
// Expone el estado de la base de datos y de los workers, pero no afecta
// al balanceo de Kong.
func RegisterDepsHealthRoute(router *gin.Engine, handler *DepsHealthHandler) {
	router.GET("/health/deps", handler.Handle)
}
