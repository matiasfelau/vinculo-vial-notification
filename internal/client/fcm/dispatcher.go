package fcm

import (
	"context"
	"time"

	"firebase.google.com/go/v4/messaging"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/breaker"
)

const (
	notificationTitle = "Alerta de Vinculo Vial"
	notificationBody  = "Tenes una nueva alerta. Abri la app para ver el detalle."
)

// multicastSender es el subconjunto del cliente de Firebase Admin SDK
// que este paquete consume. Aislarlo permite reemplazarlo por un doble
// de prueba sin credenciales reales.
type multicastSender interface {
	SendEachForMulticast(ctx context.Context, message *messaging.MulticastMessage) (*messaging.BatchResponse, error)
}

// isInvalidToken clasifica un error de envio individual segun los
// codigos que documenta FCM para un token que ya no sirve. Es una
// variable, no una funcion, para poder sustituirla en las pruebas de
// Dispatch sin depender de construir errores internos del SDK.
var isInvalidToken = func(err error) bool {
	return err != nil && (messaging.IsUnregistered(err) || messaging.IsInvalidArgument(err))
}

// Dispatcher implementa domain.Dispatcher sobre Firebase Cloud
// Messaging, particionando el envio en lotes segun el limite del
// proveedor.
type Dispatcher struct {
	client    multicastSender
	batchSize int
	timeout   time.Duration
	breaker   *breaker.Breaker
}

// NewDispatcher construye el Dispatcher. timeout acota cada llamada
// individual al SDK: sin un limite propio, una llamada colgada al
// proveedor bloquearia el worker para siempre, dejando la cola sin
// consumirse mientras el proceso sigue respondiendo HTTP con
// normalidad. timeout <= 0 desactiva el limite.
//
// cb protege la dependencia con un circuit breaker: con el circuito
// abierto, Dispatch falla de inmediato sin llamar al SDK. cb nil
// desactiva el breaker (todas las llamadas se intentan).
func NewDispatcher(client *messaging.Client, batchSize int, timeout time.Duration, cb *breaker.Breaker) *Dispatcher {
	return &Dispatcher{client: client, batchSize: batchSize, timeout: timeout, breaker: cb}
}

var _ domain.Dispatcher = (*Dispatcher)(nil)

func (d *Dispatcher) Dispatch(ctx context.Context, tokens []string, alertType string) ([]domain.DispatchOutcome, error) {
	outcomes := make([]domain.DispatchOutcome, 0, len(tokens))

	for _, batch := range partition(tokens, d.batchSize) {
		message := buildMulticastMessage(batch, alertType)

		response, err := d.sendBatch(ctx, message)
		if err != nil {
			return outcomes, err
		}

		for i, result := range response.Responses {
			outcomes = append(outcomes, domain.DispatchOutcome{
				Token:   batch[i],
				Success: result.Success,
				Invalid: isInvalidToken(result.Error),
			})
		}
	}

	return outcomes, nil
}

// sendBatch envia un unico lote. Con el breaker abierto, falla de
// inmediato sin tocar la red (ErrOpen). El resultado de la llamada al
// SDK se reporta al breaker: solo un fallo de transporte del SDK
// (timeout, error de conexion) cuenta como falla de la dependencia.
// Un rechazo de negocio para un token puntual (Unregistered,
// InvalidArgument) viaja dentro de una respuesta exitosa del SDK y se
// reporta como exito del circuito, igual que un 4xx de un upstream
// HTTP no abre un breaker HTTP.
func (d *Dispatcher) sendBatch(ctx context.Context, message *messaging.MulticastMessage) (*messaging.BatchResponse, error) {
	if d.breaker != nil {
		if err := d.breaker.Allow(); err != nil {
			return nil, err
		}
	}

	response, err := d.sendWithTimeout(ctx, message)

	if d.breaker != nil {
		d.breaker.Report(err == nil)
	}

	return response, err
}

// sendWithTimeout acota el tiempo de espera al SDK con un timeout
// propio, independiente del ctx del llamador. Cada lote recibe su
// propio presupuesto de tiempo: un despacho con varios lotes no debe
// repartir un unico timeout entre todos.
func (d *Dispatcher) sendWithTimeout(ctx context.Context, message *messaging.MulticastMessage) (*messaging.BatchResponse, error) {
	if d.timeout <= 0 {
		return d.client.SendEachForMulticast(ctx, message)
	}

	batchCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	return d.client.SendEachForMulticast(batchCtx, message)
}

// buildMulticastMessage arma el payload sin PII: solo alert_type como
// dato de enrutamiento y un texto generico. El actor del evento nunca
// llega hasta aca.
func buildMulticastMessage(batch []string, alertType string) *messaging.MulticastMessage {
	return &messaging.MulticastMessage{
		Tokens: batch,
		Data: map[string]string{
			"alert_type": alertType,
		},
		Notification: &messaging.Notification{
			Title: notificationTitle,
			Body:  notificationBody,
		},
	}
}
