package domain

import (
	"context"
	"errors"
)

// EmailMessage es un email listo para enviar: destinatario, asunto y las
// dos versiones del cuerpo (texto plano y HTML minimo). Contiene datos
// personales (To) y, segun la plantilla, un codigo de un solo uso: nunca
// se loguea ni se incluye en un error.
type EmailMessage struct {
	To       string
	Subject  string
	TextBody string
	HTMLBody string
}

// ErrEmailRejected indica que el servidor de correo rechazo el mensaje de
// forma permanente (respuesta 5xx a MAIL, RCPT o DATA): reintentar no
// cambia el resultado. Quien lo recibe confirma el mensaje y lo registra
// sin destinatario. Cualquier otro error de EmailSender es transitorio.
var ErrEmailRejected = errors.New("el servidor de correo rechazo el mensaje")

// EmailSender envia un email. La capa de dominio no conoce SMTP ni SES:
// solo depende de esta interfaz.
type EmailSender interface {
	// Send entrega message. Devuelve nil si el servidor lo acepto, un
	// error que envuelve ErrEmailRejected ante un rechazo permanente y
	// cualquier otro error ante una falla transitoria (timeout, conexion,
	// respuesta 4xx, circuito abierto). Los errores no incluyen el
	// destinatario ni el texto que devuelve el servidor.
	Send(ctx context.Context, message EmailMessage) error
}
