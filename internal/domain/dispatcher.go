package domain

import "context"

// DispatchOutcome resume, por token, el resultado del envio. Invalid
// distingue los tokens que FCM reporto como Unregistered o
// InvalidArgument: esos deben eliminarse de device_tokens en la misma
// corrida (limpieza autonoma).
type DispatchOutcome struct {
	Token   string
	Success bool
	Invalid bool
}

// Dispatcher despacha una notificacion push generica, sin PII, a un
// lote de tokens. La capa de dominio no conoce Firebase ni ningun
// otro proveedor: solo depende de esta interfaz.
type Dispatcher interface {
	// Dispatch envia la notificacion correspondiente a alertType a los
	// tokens dados, particionando internamente segun el limite del
	// proveedor, y devuelve el resultado por token.
	Dispatch(ctx context.Context, tokens []string, alertType string) ([]DispatchOutcome, error)
}
