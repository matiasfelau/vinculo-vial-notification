package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const ownerIDContextKey = "ownerID"

// OwnerIDMiddleware exige el encabezado X-User-Id, inyectado por Kong
// tras resolver el JWT del usuario, y lo deja disponible para los
// handlers. El owner_id nunca se acepta por body ni por query param.
func OwnerIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, err := uuid.Parse(c.GetHeader("X-User-Id"))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "encabezado X-User-Id ausente o invalido"})
			return
		}
		c.Set(ownerIDContextKey, ownerID)
		c.Next()
	}
}

func ownerIDFromContext(c *gin.Context) uuid.UUID {
	return c.MustGet(ownerIDContextKey).(uuid.UUID)
}
