package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/tokens"
)

func newTestRouter() (*gin.Engine, *memory.DeviceTokenRepository) {
	gin.SetMode(gin.TestMode)

	repo := memory.NewDeviceTokenRepository()
	handler := NewTokenHandler(tokens.NewRegisterTokenService(repo), tokens.NewDeleteTokenService(repo))

	router := NewRouter()
	RegisterTokenRoutes(router, handler, NewDailyRateLimiter())

	return router, repo
}

func doTokenRequest(router *gin.Engine, method, ownerID, fcmToken string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]string{"fcm_token": fcmToken})
	req := httptest.NewRequest(method, "/api/v1/notifications/tokens", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if ownerID != "" {
		req.Header.Set("X-User-Id", ownerID)
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestTokenHandler_RegisterConDatosValidosDevuelveOK(t *testing.T) {
	router, repo := newTestRouter()
	owner := uuid.New().String()

	rec := doTokenRequest(router, http.MethodPost, owner, "token-1")

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	ownerUUID, _ := uuid.Parse(owner)
	found, _ := repo.FindByOwnerIDs(context.Background(), []uuid.UUID{ownerUUID})
	if len(found) != 1 {
		t.Fatalf("se esperaba un token persistido, se obtuvo: %+v", found)
	}
}

func TestTokenHandler_RegisterSinHeaderDevuelveBadRequest(t *testing.T) {
	router, _ := newTestRouter()

	rec := doTokenRequest(router, http.MethodPost, "", "token-1")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("codigo de estado = %d, se esperaba %d", rec.Code, http.StatusBadRequest)
	}
}

func TestTokenHandler_RegisterSinFCMTokenDevuelveBadRequest(t *testing.T) {
	router, _ := newTestRouter()
	owner := uuid.New().String()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/tokens", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", owner)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("codigo de estado = %d, se esperaba %d", rec.Code, http.StatusBadRequest)
	}
}

func TestTokenHandler_RegisterSuperaElLimiteDiario(t *testing.T) {
	router, _ := newTestRouter()
	owner := uuid.New().String()

	for i := 0; i < dailyTokenRegistrationLimit; i++ {
		rec := doTokenRequest(router, http.MethodPost, owner, "token-repetido")
		if rec.Code != http.StatusOK {
			t.Fatalf("intento %d: codigo de estado = %d, se esperaba %d", i+1, rec.Code, http.StatusOK)
		}
	}

	rec := doTokenRequest(router, http.MethodPost, owner, "token-repetido")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("codigo de estado = %d, se esperaba %d", rec.Code, http.StatusTooManyRequests)
	}
}

func TestTokenHandler_DeleteDeUnTokenInexistenteDevuelveOK(t *testing.T) {
	router, _ := newTestRouter()
	owner := uuid.New().String()

	rec := doTokenRequest(router, http.MethodDelete, owner, "no-existe")

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d", rec.Code, http.StatusOK)
	}
}

func TestTokenHandler_DeleteNoEstaSujetoAlLimiteDiario(t *testing.T) {
	router, _ := newTestRouter()
	owner := uuid.New().String()

	for i := 0; i < dailyTokenRegistrationLimit+5; i++ {
		rec := doTokenRequest(router, http.MethodDelete, owner, "token-x")
		if rec.Code != http.StatusOK {
			t.Fatalf("intento %d: codigo de estado = %d, se esperaba %d", i+1, rec.Code, http.StatusOK)
		}
	}
}

func TestTokenHandler_DeleteBorraElTokenRegistrado(t *testing.T) {
	router, repo := newTestRouter()
	owner := uuid.New().String()
	ownerUUID, _ := uuid.Parse(owner)

	doTokenRequest(router, http.MethodPost, owner, "token-a-borrar")
	rec := doTokenRequest(router, http.MethodDelete, owner, "token-a-borrar")

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d", rec.Code, http.StatusOK)
	}

	found, _ := repo.FindByOwnerIDs(context.Background(), []uuid.UUID{ownerUUID})
	if len(found) != 0 {
		t.Fatalf("el token deberia haberse borrado, se obtuvo: %+v", found)
	}
}
