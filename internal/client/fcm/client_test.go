package fcm

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCredentials(t *testing.T, content []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "firebase.json")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("no se pudo escribir las credenciales de prueba: %v", err)
	}
	return path
}

func serviceAccount(t *testing.T, projectID string) []byte {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("no se pudo generar la clave de prueba: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("no se pudo serializar la clave de prueba: %v", err)
	}

	account := map[string]string{
		"type":           "service_account",
		"private_key_id": "clave-de-prueba",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "prueba@proyecto-de-prueba.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      "https://oauth2.googleapis.com/token",
	}
	if projectID != "" {
		account["project_id"] = projectID
	}

	content, err := json.Marshal(account)
	if err != nil {
		t.Fatalf("no se pudo armar el JSON de credenciales: %v", err)
	}
	return content
}

func TestNewMessagingClient_InicializaConUnServiceAccountValido(t *testing.T) {
	path := writeCredentials(t, serviceAccount(t, "proyecto-de-prueba"))

	client, err := NewMessagingClient(context.Background(), path)

	if err != nil {
		t.Fatalf("NewMessagingClient() devolvio un error inesperado: %v", err)
	}
	if client == nil {
		t.Fatal("se esperaba un cliente de messaging")
	}
}

func TestNewMessagingClient_FallaSiNoExisteElArchivoDeCredenciales(t *testing.T) {
	client, err := NewMessagingClient(context.Background(), filepath.Join(t.TempDir(), "no-existe.json"))

	// El Admin SDK no lee el archivo al crear la app: el fallo aparece
	// al pedir el cliente de messaging, sin proyecto resoluble.
	if err == nil || !strings.Contains(err.Error(), "no se pudo obtener el cliente de messaging") {
		t.Fatalf("err = %v, se esperaba el error del cliente de messaging", err)
	}
	if client != nil {
		t.Error("ante un error no debe devolverse un cliente")
	}
}

func TestNewMessagingClient_FallaSiElArchivoDeCredencialesEstaCorrupto(t *testing.T) {
	client, err := NewMessagingClient(context.Background(), writeCredentials(t, []byte("esto no es json")))

	if err == nil {
		t.Fatal("se esperaba un error con credenciales corruptas")
	}
	if client != nil {
		t.Error("ante un error no debe devolverse un cliente")
	}
}

func TestNewMessagingClient_FallaSiElServiceAccountNoTieneProyecto(t *testing.T) {
	path := writeCredentials(t, serviceAccount(t, ""))

	client, err := NewMessagingClient(context.Background(), path)

	if err == nil || !strings.Contains(err.Error(), "no se pudo obtener el cliente de messaging") {
		t.Fatalf("err = %v, se esperaba el error del cliente de messaging", err)
	}
	if client != nil {
		t.Error("ante un error no debe devolverse un cliente")
	}
}
