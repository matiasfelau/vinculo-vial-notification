package http

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/matiasfelau/vinculo-vial-notification/internal/service/tokens"
)

type tokenRequest struct {
	FCMToken string `json:"fcm_token" binding:"required"`
}

// TokenHandler expone el alta y la baja de tokens FCM del dispositivo.
type TokenHandler struct {
	registerService *tokens.RegisterTokenService
	deleteService   *tokens.DeleteTokenService
}

func NewTokenHandler(registerService *tokens.RegisterTokenService, deleteService *tokens.DeleteTokenService) *TokenHandler {
	return &TokenHandler{registerService: registerService, deleteService: deleteService}
}

// Register da de alta o actualiza el token del dispositivo del usuario
// autenticado.
func (h *TokenHandler) Register(c *gin.Context) {
	var req tokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "fcm_token es requerido"})
		return
	}

	ownerID := ownerIDFromContext(c)

	if err := h.registerService.Execute(c.Request.Context(), ownerID, req.FCMToken); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "no se pudo registrar el token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Delete da de baja el token del dispositivo. Responde 200 siempre,
// exista o no el token, para evitar enumeracion.
func (h *TokenHandler) Delete(c *gin.Context) {
	var req tokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "fcm_token es requerido"})
		return
	}

	ownerID := ownerIDFromContext(c)

	if err := h.deleteService.Execute(c.Request.Context(), ownerID, req.FCMToken); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "no se pudo dar de baja el token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
