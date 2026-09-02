package worker

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDeathCount_DevuelveCeroSinHeader(t *testing.T) {
	if count := deathCount(amqp.Table{}, "notification.alert-created.q"); count != 0 {
		t.Fatalf("count = %d, se esperaba 0", count)
	}
}

func TestDeathCount_LeeElContadorDeLaColaIndicada(t *testing.T) {
	headers := amqp.Table{
		"x-death": []interface{}{
			amqp.Table{"queue": "notification.alert-created.q", "reason": "rejected", "count": int64(3)},
			amqp.Table{"queue": "notification.alert-created.q.retry", "reason": "expired", "count": int64(3)},
		},
	}

	if count := deathCount(headers, "notification.alert-created.q"); count != 3 {
		t.Fatalf("count = %d, se esperaba 3", count)
	}
}

func TestDeathCount_IgnoraEntradasDeOtrasColas(t *testing.T) {
	headers := amqp.Table{
		"x-death": []interface{}{
			amqp.Table{"queue": "otra.cola.q", "reason": "rejected", "count": int64(10)},
		},
	}

	if count := deathCount(headers, "notification.alert-created.q"); count != 0 {
		t.Fatalf("count = %d, se esperaba 0", count)
	}
}

func TestDeathCount_ToleraFormatoInesperado(t *testing.T) {
	headers := amqp.Table{"x-death": "no es un arreglo"}
	if count := deathCount(headers, "notification.alert-created.q"); count != 0 {
		t.Fatalf("count = %d, se esperaba 0", count)
	}
}
