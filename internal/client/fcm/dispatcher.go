package fcm

import (
	"context"

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
}

func NewDispatcher(client *messaging.Client, batchSize int) *Dispatcher {
	return &Dispatcher{client: client, batchSize: batchSize}
}

var _ domain.Dispatcher = (*Dispatcher)(nil)

func (d *Dispatcher) Dispatch(ctx context.Context, tokens []string, alertType string) ([]domain.DispatchOutcome, error) {
	outcomes := make([]domain.DispatchOutcome, 0, len(tokens))

	for _, batch := range partition(tokens, d.batchSize) {
		message := buildMulticastMessage(batch, alertType)

		response, err := d.client.SendEachForMulticast(ctx, message)
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
