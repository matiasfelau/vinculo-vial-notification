package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// fakeDBPinger implementa dbPinger devolviendo siempre el error
// configurado.
type fakeDBPinger struct{ err error }

func (p fakeDBPinger) PingContext(context.Context) error { return p.err }

// fakeConnectionChecker implementa ConnectionChecker con un valor
// fijo.
type fakeConnectionChecker struct{ connected bool }

func (c fakeConnectionChecker) Connected() bool { return c.connected }

func newReadinessRouter(dbErr error, brokers map[string]ConnectionChecker) *gin.Engine {
	gin.SetMode(gin.TestMode)

	handler := NewReadinessHandler(fakeDBPinger{err: dbErr}, brokers)
	router := NewRouter()
	RegisterReadinessRoute(router, handler)

	return router
}

func doReadinessRequest(router *gin.Engine) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestReadinessHandler_TodoUp_DevuelveOK(t *testing.T) {
	router := newReadinessRouter(nil, map[string]ConnectionChecker{
		"rabbitmq_alert_created": fakeConnectionChecker{connected: true},
	})

	rec := doReadinessRequest(router)

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestReadinessHandler_BaseCaida_DevuelveServiceUnavailable(t *testing.T) {
	router := newReadinessRouter(errors.New("conexion rechazada"), map[string]ConnectionChecker{
		"rabbitmq_alert_created": fakeConnectionChecker{connected: true},
	})

	rec := doReadinessRequest(router)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

func TestReadinessHandler_BrokerDesconectado_DevuelveServiceUnavailable(t *testing.T) {
	router := newReadinessRouter(nil, map[string]ConnectionChecker{
		"rabbitmq_alert_created":    fakeConnectionChecker{connected: true},
		"rabbitmq_sessions_revoked": fakeConnectionChecker{connected: false},
	})

	rec := doReadinessRequest(router)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

func TestReadinessHandler_SinBrokersConfigurados_SoloDependeDeLaBase(t *testing.T) {
	router := newReadinessRouter(nil, map[string]ConnectionChecker{})

	rec := doReadinessRequest(router)

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}
