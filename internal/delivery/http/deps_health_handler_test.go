package http

import (
	"encoding/json"
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

type depsHealthBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func decodeDepsHealthBody(t *testing.T, rec *httptest.ResponseRecorder) depsHealthBody {
	t.Helper()

	var body depsHealthBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("el cuerpo no es JSON valido: %v. cuerpo: %s", err, rec.Body.String())
	}
	return body
}

func TestDepsHealthHandler_TodoUp_DevuelveOK(t *testing.T) {
	router := newDepsHealthRouter(nil, map[string]ConnectionChecker{
		"rabbitmq_alert_created": fakeConnectionChecker{connected: true},
	})

	rec := doDepsHealthRequest(router)

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeDepsHealthBody(t, rec)
	if body.Status != "ready" {
		t.Errorf("status = %q, se esperaba %q", body.Status, "ready")
	}
	if body.Checks["database"] != "up" || body.Checks["rabbitmq_alert_created"] != "up" {
		t.Errorf("checks = %v, se esperaba todo en up", body.Checks)
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
	body := decodeDepsHealthBody(t, rec)
	if body.Status != "degraded" {
		t.Errorf("status = %q, se esperaba %q", body.Status, "degraded")
	}
	if body.Checks["database"] != "down" || body.Checks["rabbitmq_alert_created"] != "up" {
		t.Errorf("checks = %v, se esperaba database=down y rabbitmq_alert_created=up", body.Checks)
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
	body := decodeDepsHealthBody(t, rec)
	if body.Status != "degraded" {
		t.Errorf("status = %q, se esperaba %q", body.Status, "degraded")
	}
	if body.Checks["database"] != "up" || body.Checks["rabbitmq_sessions_revoked"] != "down" {
		t.Errorf("checks = %v, se esperaba database=up y rabbitmq_sessions_revoked=down", body.Checks)
	}
}

// Un worker caido deja la instancia degradada, no fuera del balanceo
// (eso es lo que significa "not_ready" en /ready): /health/deps nunca
// debe usar ese estado.
func TestDepsHealthHandler_WorkerCaido_NoUsaNotReady(t *testing.T) {
	router := newDepsHealthRouter(nil, map[string]ConnectionChecker{
		"rabbitmq_alert_created": fakeConnectionChecker{connected: false},
	})

	body := decodeDepsHealthBody(t, doDepsHealthRequest(router))

	if body.Status == "not_ready" {
		t.Fatal(`/health/deps no debe responder "not_ready": la instancia esta lista para el balanceo`)
	}
}

func TestDepsHealthHandler_SinBrokersConfigurados_SoloDependeDeLaBase(t *testing.T) {
	router := newDepsHealthRouter(nil, map[string]ConnectionChecker{})

	rec := doDepsHealthRequest(router)

	if rec.Code != http.StatusOK {
		t.Fatalf("codigo de estado = %d, se esperaba %d. cuerpo: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}
