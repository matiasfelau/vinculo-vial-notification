// Package smtp es el adaptador de salida de EmailSender sobre SMTP. En
// local apunta a Mailpit, sin TLS; en AWS apuntara al endpoint SMTP de
// SES con STARTTLS, solo con cambio de configuracion.
package smtp

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"time"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
	"github.com/matiasfelau/vinculo-vial-notification/pkg/breaker"
)

// Config agrupa los parametros del adaptador. Username y Password vacios
// desactivan la autenticacion (Mailpit).
type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	// From es la direccion del remitente, con nombre opcional:
	// "Vinculo Vial <no-responder@vinculovial.local>".
	From string
	// StartTLS exige negociar TLS antes de autenticar y enviar. Con true,
	// un servidor que no ofrece STARTTLS es un error: nunca se degrada a
	// texto plano.
	StartTLS bool
	// Timeout acota la operacion completa (conexion, dialogo y envio).
	Timeout time.Duration
}

// Sender implementa domain.EmailSender sobre net/smtp.
type Sender struct {
	cfg     Config
	from    *mail.Address
	breaker *breaker.Breaker

	// tlsConfig se indirecciona para poder probar STARTTLS contra un
	// servidor en proceso con un certificado propio.
	tlsConfig func() *tls.Config
}

var _ domain.EmailSender = (*Sender)(nil)

// NewSender valida la configuracion y construye el adaptador. cb protege
// la dependencia con un circuit breaker: con el circuito abierto, Send
// falla de inmediato sin tocar la red. cb nil lo desactiva.
func NewSender(cfg Config, cb *breaker.Breaker) (*Sender, error) {
	if cfg.Host == "" {
		return nil, errors.New("smtp: host obligatorio")
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return nil, errors.New("smtp: puerto fuera de rango")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("smtp: timeout obligatorio y positivo")
	}
	from, err := mail.ParseAddress(cfg.From)
	if err != nil {
		return nil, errors.New("smtp: remitente invalido")
	}

	return &Sender{
		cfg:     cfg,
		from:    from,
		breaker: cb,
		tlsConfig: func() *tls.Config {
			return &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
		},
	}, nil
}

// Send entrega el mensaje. El resultado se reporta al breaker: solo una
// falla transitoria (timeout, conexion, 4xx) cuenta como falla de la
// dependencia; un rechazo permanente del servidor para este mensaje es
// una respuesta valida, igual que un 4xx de un upstream HTTP no abre un
// breaker HTTP.
func (s *Sender) Send(ctx context.Context, message domain.EmailMessage) error {
	if s.breaker != nil {
		if err := s.breaker.Allow(); err != nil {
			return err
		}
	}

	err := s.send(ctx, message)

	if s.breaker != nil {
		s.breaker.Report(err == nil || errors.Is(err, domain.ErrEmailRejected))
	}
	return err
}

func (s *Sender) send(ctx context.Context, message domain.EmailMessage) error {
	to, err := mail.ParseAddress(message.To)
	if err != nil || to.Address != message.To {
		// Nunca se incluye el destinatario en el error.
		return fmt.Errorf("%w: destinatario invalido", domain.ErrEmailRejected)
	}

	data, err := buildMessage(s.from, to.Address, message)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	address := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return sanitize(err)
	}
	defer conn.Close()

	// El plazo cubre todo el dialogo; si el contexto se cancela antes,
	// cerrar la conexion destraba cualquier lectura o escritura en curso.
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return sanitize(err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	client, err := smtp.NewClient(conn, s.cfg.Host)
	if err != nil {
		return sanitize(err)
	}
	defer client.Close()

	// Un 5xx en el saludo, STARTTLS o la autenticacion no es un rechazo
	// de este mensaje sino un problema de la conexion o de la
	// configuracion: se trata como transitorio para que quede visible en
	// el ciclo de reintentos y la DLQ en lugar de descartar el mensaje.
	if s.cfg.StartTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return errors.New("smtp: el servidor no ofrece STARTTLS")
		}
		if err := client.StartTLS(s.tlsConfig()); err != nil {
			return sanitize(err)
		}
	}

	if s.cfg.Username != "" {
		auth := smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)
		if err := client.Auth(auth); err != nil {
			return sanitize(err)
		}
	}

	if err := client.Mail(s.from.Address); err != nil {
		return classify(err)
	}
	if err := client.Rcpt(to.Address); err != nil {
		return classify(err)
	}

	writer, err := client.Data()
	if err != nil {
		return classify(err)
	}
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return sanitize(err)
	}
	if err := writer.Close(); err != nil {
		return classify(err)
	}

	// Un fallo de QUIT no afecta al envio ya aceptado.
	_ = client.Quit()
	return nil
}

// classify traduce un error del dialogo de envio (MAIL, RCPT, DATA): una
// respuesta 5xx es un rechazo permanente; cualquier otra cosa es
// transitoria.
func classify(err error) error {
	var protoErr *textproto.Error
	if errors.As(err, &protoErr) && protoErr.Code >= 500 && protoErr.Code <= 599 {
		return fmt.Errorf("%w: codigo %d", domain.ErrEmailRejected, protoErr.Code)
	}
	return sanitize(err)
}

// sanitize reduce un error de net/smtp a algo seguro de loguear. El
// texto de una respuesta SMTP lo escribe el servidor y suele repetir el
// destinatario ("550 <usuario@dominio>: Recipient address rejected"), asi
// que solo se conserva el codigo.
func sanitize(err error) error {
	var protoErr *textproto.Error
	if errors.As(err, &protoErr) {
		return fmt.Errorf("smtp: respuesta %d del servidor", protoErr.Code)
	}
	return err
}

// randomToken devuelve 16 bytes aleatorios en hexadecimal, para el
// Message-ID y el limite MIME.
func randomToken() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("smtp: no se pudo generar un identificador: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
