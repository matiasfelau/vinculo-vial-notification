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

func TestDeathCount_CuentaSoloLaColaDeTrabajoAunqueLaRetryTengaOtroContador(t *testing.T) {
	const queue = "notification.alert-created.q"
	headers := amqp.Table{
		"x-death": []interface{}{
			amqp.Table{"queue": "notification.alert-created.retry", "reason": "expired", "count": int64(9)},
			amqp.Table{"queue": queue, "reason": "rejected", "count": int64(2)},
		},
	}

	if count := deathCount(headers, queue); count != 2 {
		t.Fatalf("count = %d, se esperaba 2 (solo las entradas de la cola de trabajo)", count)
	}
}

func TestDeathCount_AceptaContadoresDeCualquierTipoEnteroYTomaElMayor(t *testing.T) {
	const queue = "notification.alert-created.q"

	cases := []struct {
		name  string
		count interface{}
		want  int64
	}{
		{"int64", int64(4), 4},
		{"int32", int32(5), 5},
		{"int", 6, 6},
		{"tipo no numerico", "7", 0},
		{"ausente", nil, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			headers := amqp.Table{"x-death": []interface{}{
				amqp.Table{"queue": queue, "count": int64(1)},
				amqp.Table{"queue": queue, "count": tc.count},
			}}

			want := tc.want
			if want < 1 {
				want = 1
			}
			if got := deathCount(headers, queue); got != want {
				t.Fatalf("deathCount = %d, se esperaba %d", got, want)
			}
		})
	}
}

func TestDeathCount_IgnoraEntradasQueNoSonTablas(t *testing.T) {
	const queue = "notification.alert-created.q"
	headers := amqp.Table{"x-death": []interface{}{
		"basura",
		amqp.Table{"queue": queue, "count": int64(2)},
	}}

	if got := deathCount(headers, queue); got != 2 {
		t.Fatalf("deathCount = %d, se esperaba 2: una entrada invalida no debe anular las demas", got)
	}
}
