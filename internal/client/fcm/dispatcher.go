package fcm

import (
	"context"
	"time"

	"firebase.google.com/go/v4/messaging"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
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
}

// NewDispatcher construye el Dispatcher. timeout acota cada llamada
// individual al SDK: sin un limite propio, una llamada colgada al
// proveedor bloquearia el worker para siempre, dejando la cola sin
// consumirse mientras el proceso sigue respondiendo HTTP con
// normalidad. timeout <= 0 desactiva el limite.
func NewDispatcher(client *messaging.Client, batchSize int, timeout time.Duration) *Dispatcher {
	return &Dispatcher{client: client, batchSize: batchSize, timeout: timeout}
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

// sendBatch envia un unico lote acotando el tiempo de espera al SDK
// con un timeout propio, independiente del ctx del llamador. Cada
// lote recibe su propio presupuesto de tiempo: un despacho con varios
// lotes no debe repartir un unico timeout entre todos.
func (d *Dispatcher) sendBatch(ctx context.Context, message *messaging.MulticastMessage) (*messaging.BatchResponse, error) {
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
