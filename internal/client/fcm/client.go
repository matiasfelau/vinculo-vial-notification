package fcm

import (
	"context"
	"fmt"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

// NewMessagingClient inicializa el Admin SDK de Firebase a partir del
// JSON de credenciales del service account, inyectado por variable de
// entorno, y devuelve el cliente de Messaging.
func NewMessagingClient(ctx context.Context, credentialsJSON string) (*messaging.Client, error) {
	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsJSON([]byte(credentialsJSON)))
	if err != nil {
		return nil, fmt.Errorf("no se pudo inicializar la app de firebase: %w", err)
	}

	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("no se pudo obtener el cliente de messaging: %w", err)
	}

	return client, nil
}
