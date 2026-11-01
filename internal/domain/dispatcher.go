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

// PushMessage es el contenido de una notificacion push: el texto visible
// y los datos de enrutamiento que la app usa para el deep link. No debe
// contener PII; Data puede ser nil cuando no hay deep link.
type PushMessage struct {
	Title string
	Body  string
	Data  map[string]string
}

// Dispatcher despacha una notificacion push generica, sin PII, a un
// lote de tokens. La capa de dominio no conoce Firebase ni ningun
// otro proveedor: solo depende de esta interfaz.
type Dispatcher interface {
	// Dispatch envia message a los tokens dados, particionando
	// internamente segun el limite del proveedor, y devuelve el
	// resultado por token.
	Dispatch(ctx context.Context, tokens []string, message PushMessage) ([]DispatchOutcome, error)
}
