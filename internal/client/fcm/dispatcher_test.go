package fcm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"firebase.google.com/go/v4/messaging"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/breaker"
)

// fakeMulticastSender responde con la secuencia de resultados
// configurada, una entrada por llamada. Reemplaza al cliente real del
// SDK en las pruebas del particionado y la clasificacion de
// resultados.
type fakeMulticastSender struct {
	calls     [][]string
	responses []*messaging.BatchResponse
	errs      []error
}

func (f *fakeMulticastSender) SendEachForMulticast(_ context.Context, message *messaging.MulticastMessage) (*messaging.BatchResponse, error) {
	index := len(f.calls)
	f.calls = append(f.calls, message.Tokens)

	if index < len(f.errs) && f.errs[index] != nil {
		return nil, f.errs[index]
	}
	return f.responses[index], nil
}

func successResponses(n int) *messaging.BatchResponse {
	responses := make([]*messaging.SendResponse, n)
	for i := range responses {
		responses[i] = &messaging.SendResponse{Success: true}
	}
	return &messaging.BatchResponse{Responses: responses, SuccessCount: n}
}

func TestDispatcher_ParticionaEnLotesDe500(t *testing.T) {
	tokens := make([]string, 1200)
	for i := range tokens {
		tokens[i] = "token"
	}

	sender := &fakeMulticastSender{
		responses: []*messaging.BatchResponse{
			successResponses(500),
			successResponses(500),
			successResponses(200),
		},
	}
	dispatcher := &Dispatcher{client: sender, batchSize: 500}

	outcomes, err := dispatcher.Dispatch(context.Background(), tokens, "choque")
	if err != nil {
		t.Fatalf("Dispatch() devolvio un error inesperado: %v", err)
	}

	if len(sender.calls) != 3 {
		t.Fatalf("se esperaban 3 llamadas al SDK, se hicieron %d", len(sender.calls))
	}
	if len(sender.calls[0]) != 500 || len(sender.calls[1]) != 500 || len(sender.calls[2]) != 200 {
		t.Fatalf("particionado incorrecto: %d/%d/%d", len(sender.calls[0]), len(sender.calls[1]), len(sender.calls[2]))
	}
	if len(outcomes) != 1200 {
		t.Fatalf("se esperaban 1200 resultados, se obtuvieron %d", len(outcomes))
	}
}

func TestDispatcher_ClasificaLosTokensInvalidosSegunElClasificador(t *testing.T) {
	original := isInvalidToken
	defer func() { isInvalidToken = original }()
	isInvalidToken = func(err error) bool { return err != nil && err.Error() == "invalido" }

	sender := &fakeMulticastSender{
		responses: []*messaging.BatchResponse{
			{
				Responses: []*messaging.SendResponse{
					{Success: true},
					{Success: false, Error: errors.New("invalido")},
					{Success: false, Error: errors.New("transitorio")},
				},
			},
		},
	}
	dispatcher := &Dispatcher{client: sender, batchSize: 500}

	outcomes, err := dispatcher.Dispatch(context.Background(), []string{"a", "b", "c"}, "choque")
	if err != nil {
		t.Fatalf("Dispatch() devolvio un error inesperado: %v", err)
	}

	want := []domain.DispatchOutcome{
		{Token: "a", Success: true, Invalid: false},
		{Token: "b", Success: false, Invalid: true},
		{Token: "c", Success: false, Invalid: false},
	}
	for i, w := range want {
		if outcomes[i] != w {
			t.Errorf("outcome[%d] = %+v, se esperaba %+v", i, outcomes[i], w)
		}
	}
}

func TestDispatcher_PropagaElErrorDelSDKConservandoLosLotesPrevios(t *testing.T) {
	tokens := make([]string, 600)
	for i := range tokens {
		tokens[i] = "token"
	}

	sender := &fakeMulticastSender{
		responses: []*messaging.BatchResponse{successResponses(500), nil},
		errs:      []error{nil, errors.New("fcm no disponible")},
	}
	dispatcher := &Dispatcher{client: sender, batchSize: 500}

	outcomes, err := dispatcher.Dispatch(context.Background(), tokens, "choque")
	if err == nil {
		t.Fatal("se esperaba un error del segundo lote")
	}
	if len(outcomes) != 500 {
		t.Fatalf("se esperaban los 500 resultados del primer lote, se obtuvieron %d", len(outcomes))
	}
}

func TestBuildMulticastMessage_NoContieneActorNiIdentificadoresDeEmisor(t *testing.T) {
	message := buildMulticastMessage([]string{"token-del-dispositivo"}, "choque")

	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("no se pudo serializar el mensaje: %v", err)
	}

	lower := strings.ToLower(string(encoded))
	forbidden := []string{"actor", "issuer", "emisor", "owner_id", "patente", "dni", "license_plate"}
	for _, term := range forbidden {
		if strings.Contains(lower, term) {
			t.Errorf("el mensaje serializado contiene el termino prohibido %q: %s", term, encoded)
		}
	}

	if message.Data["alert_type"] != "choque" {
		t.Errorf("Data[alert_type] = %q, se esperaba %q", message.Data["alert_type"], "choque")
	}
	if len(message.Data) != 1 {
		t.Errorf("Data deberia contener unicamente alert_type, se obtuvo: %v", message.Data)
	}
}

// blockingMulticastSender simula un SDK colgado: nunca responde por si
// mismo, solo cuando el contexto que recibe se cancela. Permite
// comprobar que Dispatch aplica su propio timeout sin depender de que
// el proveedor real tarde de verdad.
type blockingMulticastSender struct{}

func (b *blockingMulticastSender) SendEachForMulticast(ctx context.Context, _ *messaging.MulticastMessage) (*messaging.BatchResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestDispatcher_AplicaUnTimeoutPropioPorLote(t *testing.T) {
	dispatcher := &Dispatcher{client: &blockingMulticastSender{}, batchSize: 500, timeout: 20 * time.Millisecond}

	start := time.Now()
	_, err := dispatcher.Dispatch(context.Background(), []string{"token"}, "choque")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("se esperaba un error por timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, se esperaba context.DeadlineExceeded", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Dispatch() tardo %v, no respeto el timeout configurado", elapsed)
	}
}

func TestDispatcher_SinTimeoutConfiguradoUsaElContextoDelLlamador(t *testing.T) {
	sender := &fakeMulticastSender{responses: []*messaging.BatchResponse{successResponses(1)}}
	dispatcher := &Dispatcher{client: sender, batchSize: 500, timeout: 0}

	_, err := dispatcher.Dispatch(context.Background(), []string{"token"}, "choque")
	if err != nil {
		t.Fatalf("Dispatch() devolvio un error inesperado: %v", err)
	}
}

func TestDispatcher_ConElCircuitoAbierto_NoLlamaAlSDK(t *testing.T) {
	cb := breaker.New(breaker.Config{FailureThreshold: 1, Window: time.Minute, OpenDuration: time.Minute})
	cb.Report(false) // una sola falla alcanza el umbral de 1 y abre el circuito

	sender := &fakeMulticastSender{}
	dispatcher := &Dispatcher{client: sender, batchSize: 500, breaker: cb}

	_, err := dispatcher.Dispatch(context.Background(), []string{"token"}, "choque")

	if !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("error = %v, se esperaba breaker.ErrOpen", err)
	}
	if len(sender.calls) != 0 {
		t.Fatalf("se esperaban 0 llamadas al SDK con el circuito abierto, se hicieron %d", len(sender.calls))
	}
}

func TestDispatcher_FalloDeTransporteDelSDK_CuentaComoFallaParaElBreaker(t *testing.T) {
	cb := breaker.New(breaker.Config{FailureThreshold: 1, Window: time.Minute, OpenDuration: time.Hour})
	sender := &fakeMulticastSender{
		responses: []*messaging.BatchResponse{nil},
		errs:      []error{errors.New("fcm no disponible")},
	}
	dispatcher := &Dispatcher{client: sender, batchSize: 500, breaker: cb}

	if _, err := dispatcher.Dispatch(context.Background(), []string{"token"}, "choque"); err == nil {
		t.Fatal("se esperaba que se propagara el error del SDK")
	}

	if cb.State() != breaker.StateOpen {
		t.Fatalf("estado del breaker tras el fallo = %v, se esperaba StateOpen", cb.State())
	}
}

func TestDispatcher_RechazoPorTokenInvalido_NoCuentaComoFallaParaElBreaker(t *testing.T) {
	cb := breaker.New(breaker.Config{FailureThreshold: 1, Window: time.Minute, OpenDuration: time.Hour})
	sender := &fakeMulticastSender{
		responses: []*messaging.BatchResponse{
			{
				Responses: []*messaging.SendResponse{
					{Success: false, Error: errors.New("unregistered")},
				},
			},
		},
	}
	dispatcher := &Dispatcher{client: sender, batchSize: 500, breaker: cb}

	// El SDK respondio con exito a nivel de transporte: el rechazo es
	// por token, no por disponibilidad del proveedor.
	if _, err := dispatcher.Dispatch(context.Background(), []string{"token"}, "choque"); err != nil {
		t.Fatalf("Dispatch() devolvio un error inesperado: %v", err)
	}

	if cb.State() != breaker.StateClosed {
		t.Fatalf("un rechazo por token no deberia abrir el breaker, estado = %v", cb.State())
	}
}

func TestDispatcher_SinBreakerConfigurado_SiempreIntentaLaLlamada(t *testing.T) {
	sender := &fakeMulticastSender{responses: []*messaging.BatchResponse{successResponses(1)}}
	dispatcher := &Dispatcher{client: sender, batchSize: 500, breaker: nil}

	if _, err := dispatcher.Dispatch(context.Background(), []string{"token"}, "choque"); err != nil {
		t.Fatalf("Dispatch() devolvio un error inesperado: %v", err)
	}
	if len(sender.calls) != 1 {
		t.Fatalf("se esperaba 1 llamada al SDK, se hicieron %d", len(sender.calls))
	}
}
