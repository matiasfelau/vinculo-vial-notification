package smtp

import (
	"bytes"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net/mail"
	"strings"
	"time"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

// now se indirecciona para fijar el encabezado Date en las pruebas.
var now = time.Now

// buildMessage arma el mensaje RFC 5322 como multipart/alternative: texto
// plano y HTML, ambos en UTF-8 con quoted-printable. No agrega imagenes
// remotas ni enlaces de seguimiento. Los encabezados se construyen a
// partir de direcciones ya parseadas y de un asunto codificado, de modo
// que ningun valor externo puede inyectar un salto de linea.
func buildMessage(from *mail.Address, to string, message domain.EmailMessage) ([]byte, error) {
	if strings.ContainsAny(message.Subject, "\r\n") {
		return nil, fmt.Errorf("smtp: asunto invalido")
	}

	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	boundary, err := randomToken()
	if err != nil {
		return nil, err
	}

	domainPart := from.Address[strings.LastIndex(from.Address, "@")+1:]

	var buf bytes.Buffer
	header := func(name, value string) { fmt.Fprintf(&buf, "%s: %s\r\n", name, value) }

	header("From", from.String())
	header("To", (&mail.Address{Address: to}).String())
	header("Subject", mime.QEncoding.Encode("utf-8", message.Subject))
	header("Date", now().Format(time.RFC1123Z))
	header("Message-ID", fmt.Sprintf("<%s@%s>", token, domainPart))
	header("MIME-Version", "1.0")
	header("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", boundary))
	buf.WriteString("\r\n")

	writePart := func(contentType, body string) error {
		fmt.Fprintf(&buf, "--%s\r\n", boundary)
		fmt.Fprintf(&buf, "Content-Type: %s; charset=utf-8\r\n", contentType)
		buf.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")
		writer := quotedprintable.NewWriter(&buf)
		if _, err := writer.Write([]byte(body)); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
		buf.WriteString("\r\n")
		return nil
	}

	if err := writePart("text/plain", message.TextBody); err != nil {
		return nil, err
	}
	if err := writePart("text/html", message.HTMLBody); err != nil {
		return nil, err
	}
	fmt.Fprintf(&buf, "--%s--\r\n", boundary)

	return buf.Bytes(), nil
}
