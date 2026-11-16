// Package email contiene el caso de uso del canal de email: validar el
// pedido, armar el mensaje a partir de la plantilla y enviarlo una unica
// vez por event_id.
package email

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	htmltemplate "html/template"
	"net/mail"
	"strings"
	texttemplate "text/template"
	"time"
	// La imagen final es scratch: sin la base de zonas horarias del
	// sistema, time.LoadLocation fallaria. Se incrusta en el binario.
	_ "time/tzdata"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

//go:embed templates/*.tmpl
var templateFS embed.FS

// Template identifica una de las plantillas del contrato. El valor es el
// que viaja en el campo template del payload de email.requested.
type Template string

const (
	TemplateEmailVerification Template = "EMAIL_VERIFICATION"
	TemplateAccountExists     Template = "ACCOUNT_EXISTS"
	TemplatePasswordReset     Template = "PASSWORD_RESET"
	TemplatePasswordChanged   Template = "PASSWORD_CHANGED"
)

// ErrInvalidRequest indica que el pedido no se puede convertir en un
// email (plantilla desconocida, destinatario o codigo invalidos). Es un
// error permanente: reintentar no cambia el resultado. Los mensajes que
// lo envuelven nunca incluyen el destinatario ni el codigo.
var ErrInvalidRequest = errors.New("pedido de email invalido")

// maxCodeLength acota el codigo que se acepta incrustar en el cuerpo.
const maxCodeLength = 12

// Request es el pedido de un email: el payload de email.requested mas el
// instante del evento, que PASSWORD_CHANGED muestra como fecha del
// cambio.
type Request struct {
	Template   Template
	To         string
	Code       *string
	ExpiresAt  *time.Time
	OccurredAt time.Time
}

type templateSpec struct {
	subject   string
	needsCode bool
	text      *texttemplate.Template
	html      *htmltemplate.Template
}

// templateData son los valores que reciben las plantillas.
type templateData struct {
	Code    string
	Minutes int
	Date    string
	Time    string
}

var specs = mustLoadSpecs()

func mustLoadSpecs() map[Template]templateSpec {
	definitions := []struct {
		name      Template
		subject   string
		needsCode bool
	}{
		{TemplateEmailVerification, "Tu código de Vinculo Vial", true},
		{TemplateAccountExists, "Ya tenés una cuenta en Vinculo Vial", false},
		{TemplatePasswordReset, "Recuperá tu contraseña", true},
		{TemplatePasswordChanged, "Cambiaste tu contraseña", false},
	}

	loaded := make(map[Template]templateSpec, len(definitions))
	for _, def := range definitions {
		text, err := texttemplate.ParseFS(templateFS, "templates/"+string(def.name)+".txt.tmpl")
		if err != nil {
			panic(fmt.Sprintf("plantilla de texto %s invalida: %v", def.name, err))
		}
		html, err := htmltemplate.ParseFS(templateFS, "templates/"+string(def.name)+".html.tmpl")
		if err != nil {
			panic(fmt.Sprintf("plantilla html %s invalida: %v", def.name, err))
		}
		loaded[def.name] = templateSpec{subject: def.subject, needsCode: def.needsCode, text: text, html: html}
	}
	return loaded
}

var buenosAires = mustLoadLocation("America/Argentina/Buenos_Aires")

func mustLoadLocation(name string) *time.Location {
	location, err := time.LoadLocation(name)
	if err != nil {
		panic(fmt.Sprintf("zona horaria %s no disponible: %v", name, err))
	}
	return location
}

// Render valida el pedido y arma el mensaje. now es el instante de
// referencia: el vencimiento se muestra como los minutos que faltan desde
// ahora (redondeados hacia arriba, minimo uno) y es la fecha de
// PASSWORD_CHANGED cuando el evento no trae instante.
func Render(request Request, now time.Time) (domain.EmailMessage, error) {
	spec, ok := specs[request.Template]
	if !ok {
		return domain.EmailMessage{}, fmt.Errorf("%w: plantilla desconocida", ErrInvalidRequest)
	}

	address, err := mail.ParseAddress(request.To)
	if err != nil || address.Address != request.To || strings.ContainsAny(request.To, "\r\n") {
		return domain.EmailMessage{}, fmt.Errorf("%w: destinatario invalido", ErrInvalidRequest)
	}

	data := templateData{}
	if spec.needsCode {
		if request.Code == nil || !validCode(*request.Code) {
			return domain.EmailMessage{}, fmt.Errorf("%w: codigo ausente o invalido", ErrInvalidRequest)
		}
		if request.ExpiresAt == nil {
			return domain.EmailMessage{}, fmt.Errorf("%w: la plantilla requiere expires_at", ErrInvalidRequest)
		}
		data.Code = *request.Code
		data.Minutes = remainingMinutes(*request.ExpiresAt, now)
	}

	if request.Template == TemplatePasswordChanged {
		occurredAt := request.OccurredAt
		if occurredAt.IsZero() {
			occurredAt = now
		}
		local := occurredAt.In(buenosAires)
		data.Date = local.Format("02/01/2006")
		data.Time = local.Format("15:04")
	}

	var text bytes.Buffer
	if err := spec.text.Execute(&text, data); err != nil {
		return domain.EmailMessage{}, fmt.Errorf("no se pudo armar el cuerpo de texto de %s: %w", request.Template, err)
	}
	var html bytes.Buffer
	if err := spec.html.Execute(&html, data); err != nil {
		return domain.EmailMessage{}, fmt.Errorf("no se pudo armar el cuerpo html de %s: %w", request.Template, err)
	}

	return domain.EmailMessage{
		To:       request.To,
		Subject:  spec.subject,
		TextBody: text.String(),
		HTMLBody: html.String(),
	}, nil
}

func validCode(code string) bool {
	if code == "" || len(code) > maxCodeLength {
		return false
	}
	for _, r := range code {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func remainingMinutes(expiresAt, now time.Time) int {
	remaining := expiresAt.Sub(now)
	minutes := int((remaining + time.Minute - 1) / time.Minute)
	if minutes < 1 {
		return 1
	}
	return minutes
}
