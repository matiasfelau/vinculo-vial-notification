package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/internal/repository/memory"
	"github.com/matiasfelau/vinculo-vial-notification/internal/service/tokens"
)

// erroringTokenRepository devuelve siempre el mismo error configurado,
// sin importar la operacion. Permite probar la clasificacion de
// errores de TokenHandler sin depender de una base de datos real.
type erroringTokenRepository struct {
	err error
}

func (r *erroringTokenRepository) Upsert(context.Context, *domain.DeviceToken) error {
	return r.err
}

func (r *erroringTokenRepository) Delete(context.Context, uuid.UUID, string) error {
	return r.err
}

func (r *erroringTokenRepository) FindByOwnerIDs(context.Context, []uuid.UUID) ([]domain.DeviceToken, error) {
	return nil, r.err
}

func (r *erroringTokenRepository) DeleteByTokens(context.Context, []string) error {
	return r.err
}

func (r *erroringTokenRepository) DeleteByOwnerID(context.Context, uuid.UUID) error {
	return r.err
}

func newFailingTestRouter(err error) *gin.Engine {
	gin.SetMode(gin.TestMode)

	repo := &erroringTokenRepository{err: err}
	handler := NewTokenHandler(tokens.NewRegisterTokenService(repo), tokens.NewDeleteTokenService(repo))

	router := NewRouter()
	RegisterTokenRoutes(router, handler)

	return router
}

func newTestRouter() (*gin.Engine, *memory.DeviceTokenRepository) {
	gin.SetMode(gin.TestMode)

	repo := memory.NewDeviceTokenRepository()
	handler := NewTokenHandler(tokens.NewRegisterTokenService(repo), tokens.NewDeleteTokenService(repo))

	router := NewRouter()
	RegisterTokenRoutes(router, handler)

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

func TestTokenHandler_DeleteDeUnTokenInexistenteDevuelveOK(t *testing.T) {
	router, _ := newTestRouter()
	owner := uuid.New().String()

	rec := doTokenRequest(router, http.MethodDelete, owner, "no-existe")

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d", rec.Code, http.StatusOK)
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

func TestTokenHandler_RegisterAnteDependenciaCaida_DevuelveServiceUnavailable(t *testing.T) {
	router := newFailingTestRouter(context.DeadlineExceeded)

	rec := doTokenRequest(router, http.MethodPost, uuid.New().String(), "token-1")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

func TestTokenHandler_RegisterAnteErrorInesperadoDeLaBase_DevuelveInternalServerError(t *testing.T) {
	router := newFailingTestRouter(errors.New("violacion de constraint inesperada"))

	rec := doTokenRequest(router, http.MethodPost, uuid.New().String(), "token-1")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestTokenHandler_DeleteAnteDependenciaCaida_DevuelveServiceUnavailable(t *testing.T) {
	router := newFailingTestRouter(context.DeadlineExceeded)

	rec := doTokenRequest(router, http.MethodDelete, uuid.New().String(), "token-1")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

func TestTokenHandler_DeleteAnteErrorInesperadoDeLaBase_DevuelveInternalServerError(t *testing.T) {
	router := newFailingTestRouter(errors.New("violacion de constraint inesperada"))

	rec := doTokenRequest(router, http.MethodDelete, uuid.New().String(), "token-1")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestTokenHandler_DeleteSinFCMTokenDevuelveBadRequestYNoBorraNada(t *testing.T) {
	router, repo := newTestRouter()
	owner := uuid.New()
	_ = repo.Upsert(context.Background(), &domain.DeviceToken{ID: uuid.New(), OwnerID: owner, FCMToken: "token-1"})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/notifications/tokens", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", owner.String())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("codigo de estado = %d, se esperaba %d", rec.Code, http.StatusBadRequest)
	}
	found, _ := repo.FindByOwnerIDs(context.Background(), []uuid.UUID{owner})
	if len(found) != 1 {
		t.Fatalf("una baja invalida no debe borrar tokens, quedaron %d", len(found))
	}
}
