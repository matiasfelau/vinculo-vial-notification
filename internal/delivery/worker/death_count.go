package worker

import amqp "github.com/rabbitmq/amqp091-go"

// deathCount lee el header x-death que RabbitMQ agrega cada vez que un
// mensaje es rechazado o expira desde una cola, y devuelve cuantas
// veces ocurrio eso especificamente para la cola dada. Un mensaje que
// nunca fue rechazado no trae x-death: devuelve 0.
func deathCount(headers amqp.Table, queue string) int64 {
	raw, ok := headers["x-death"]
	if !ok {
		return 0
	}

	deaths, ok := raw.([]interface{})
	if !ok {
		return 0
	}

	var count int64
	for _, entry := range deaths {
		table, ok := entry.(amqp.Table)
		if !ok {
			continue
		}
		if entryQueue, _ := table["queue"].(string); entryQueue != queue {
			continue
		}
		if c := asInt64(table["count"]); c > count {
			count = c
		}
	}
	return count
}

func asInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	default:
		return 0
	}
}
