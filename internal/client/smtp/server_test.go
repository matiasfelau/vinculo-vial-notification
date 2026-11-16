package smtp

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// receivedMessage es lo que el servidor de prueba recibio en una sesion.
type receivedMessage struct {
	from     string
	rcpts    []string
	data     string
	tls      bool
	authUser string
	authPass string
}

// testServer es un servidor SMTP minimo en proceso: alcanza para ejercitar
// el dialogo completo (EHLO, STARTTLS, AUTH PLAIN, MAIL, RCPT, DATA) y
// para forzar una respuesta distinta en cada paso.
type testServer struct {
	t        *testing.T
	listener net.Listener

	// tlsConfig no nil anuncia STARTTLS.
	tlsConfig *tls.Config
	// advertiseAuth anuncia AUTH PLAIN.
	advertiseAuth bool
	// greeting reemplaza el saludo inicial (por defecto "220 test ESMTP").
	greeting string
	// replies reemplaza la respuesta a un verbo: EHLO, STARTTLS, AUTH,
	// MAIL, RCPT, DATA (respuesta a DATA) o END (respuesta al fin de datos).
	replies map[string]string
	// hang hace que el servidor acepte la conexion y no responda nunca.
	hang bool

	connections atomic.Int32

	mu       sync.Mutex
	messages []receivedMessage
}

func newTestServer(t *testing.T, configure func(*testServer)) *testServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("no se pudo abrir el servidor de prueba: %v", err)
	}

	s := &testServer{t: t, listener: listener, replies: map[string]string{}}
	if configure != nil {
		configure(s)
	}

	t.Cleanup(func() { _ = listener.Close() })
	go s.serve()
	return s
}

func (s *testServer) port() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

func (s *testServer) received() []receivedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]receivedMessage(nil), s.messages...)
}

func (s *testServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.connections.Add(1)
		go s.handle(conn)
	}
}

func (s *testServer) reply(verb, fallback string) string {
	if override, ok := s.replies[verb]; ok {
		return override
	}
	return fallback
}

func (s *testServer) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	write := func(w net.Conn, line string) { _, _ = w.Write([]byte(line + "\r\n")) }

	if s.hang {
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
		return
	}

	greeting := s.greeting
	if greeting == "" {
		greeting = "220 test ESMTP"
	}
	write(conn, greeting)
	if strings.HasPrefix(greeting, "5") {
		return
	}

	var (
		active  net.Conn = conn
		reader           = bufio.NewReader(conn)
		current receivedMessage
	)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		verb := strings.ToUpper(strings.SplitN(line, " ", 2)[0])

		switch verb {
		case "EHLO", "HELO":
			if override, ok := s.replies["EHLO"]; ok {
				write(active, override)
				continue
			}
			caps := []string{"250-test", "250-8BITMIME"}
			if _, isTLS := active.(*tls.Conn); s.tlsConfig != nil && !isTLS {
				caps = append(caps, "250-STARTTLS")
			}
			if s.advertiseAuth {
				caps = append(caps, "250-AUTH PLAIN")
			}
			caps = append(caps, "250 SIZE 10485760")
			for _, c := range caps {
				write(active, c)
			}
		case "STARTTLS":
			response := s.reply("STARTTLS", "220 listo para TLS")
			write(active, response)
			if !strings.HasPrefix(response, "220") {
				continue
			}
			tlsConn := tls.Server(active, s.tlsConfig)
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			active = tlsConn
			reader = bufio.NewReader(tlsConn)
			current.tls = true
		case "AUTH":
			fields := strings.Fields(line)
			if len(fields) == 3 {
				if raw, err := base64.StdEncoding.DecodeString(fields[2]); err == nil {
					parts := strings.Split(string(raw), "\x00")
					if len(parts) == 3 {
						current.authUser, current.authPass = parts[1], parts[2]
					}
				}
			}
			write(active, s.reply("AUTH", "235 2.7.0 autenticado"))
		case "MAIL":
			response := s.reply("MAIL", "250 ok")
			current.from = between(line, '<', '>')
			write(active, response)
		case "RCPT":
			response := s.reply("RCPT", "250 ok")
			if strings.HasPrefix(response, "2") {
				current.rcpts = append(current.rcpts, between(line, '<', '>'))
			}
			write(active, response)
		case "DATA":
			response := s.reply("DATA", "354 enviar datos")
			write(active, response)
			if !strings.HasPrefix(response, "354") {
				continue
			}
			var data strings.Builder
			for {
				dataLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if dataLine == ".\r\n" {
					break
				}
				data.WriteString(strings.TrimPrefix(dataLine, "."))
			}
			current.data = data.String()
			response = s.reply("END", "250 2.0.0 aceptado")
			if strings.HasPrefix(response, "2") {
				s.mu.Lock()
				s.messages = append(s.messages, current)
				s.mu.Unlock()
				current = receivedMessage{tls: current.tls}
			}
			write(active, response)
		case "QUIT":
			write(active, "221 chau")
			return
		default:
			write(active, "502 comando no implementado")
		}
	}
}

func between(s string, open, close byte) string {
	start := strings.IndexByte(s, open)
	end := strings.IndexByte(s, close)
	if start < 0 || end < start {
		return ""
	}
	return s[start+1 : end]
}

// selfSignedCert genera un certificado para 127.0.0.1 y el pool que lo
// confia, para probar STARTTLS sin saltear la verificacion.
func selfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("no se pudo generar la clave: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("no se pudo crear el certificado: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("no se pudo parsear el certificado: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}
