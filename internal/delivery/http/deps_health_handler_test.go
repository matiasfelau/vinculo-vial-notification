package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func newDepsHealthRouter(dbErr error, brokers map[string]ConnectionChecker) *gin.Engine {
	gin.SetMode(gin.TestMode)

	handler := NewDepsHealthHandler(fakeDBPinger{err: dbErr}, brokers)
	router := NewRouter()
	RegisterDepsHealthRoute(router, handler)

	return router
}

func doDepsHealthRequest(router *gin.Engine) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/health/deps", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestDepsHealthHandler_TodoUp_DevuelveOK(t *testing.T) {
	router := newDepsHealthRouter(nil, map[string]ConnectionChecker{
		"rabbitmq_alert_created": fakeConnectionChecker{connected: true},
	})

	rec := doDepsHealthRequest(router)

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestDepsHealthHandler_BaseCaida_DevuelveServiceUnavailable(t *testing.T) {
	router := newDepsHealthRouter(errors.New("conexion rechazada"), map[string]ConnectionChecker{
		"rabbitmq_alert_created": fakeConnectionChecker{connected: true},
	})

	rec := doDepsHealthRequest(router)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

func TestDepsHealthHandler_BrokerDesconectado_DevuelveServiceUnavailable(t *testing.T) {
	router := newDepsHealthRouter(nil, map[string]ConnectionChecker{
		"rabbitmq_alert_created":    fakeConnectionChecker{connected: true},
		"rabbitmq_sessions_revoked": fakeConnectionChecker{connected: false},
	})

	rec := doDepsHealthRequest(router)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

func TestDepsHealthHandler_SinBrokersConfigurados_SoloDependeDeLaBase(t *testing.T) {
	router := newDepsHealthRouter(nil, map[string]ConnectionChecker{})

	rec := doDepsHealthRequest(router)

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}
