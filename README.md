# vinculo-vial-notification

Servicio de notificaciones push de la plataforma Vinculo Vial. Expone una
API HTTP para el registro y baja de tokens de dispositivo (FCM) y ejecuta,
en el mismo proceso, un worker que consume eventos de RabbitMQ para
despachar notificaciones push a traves de Firebase Cloud Messaging.

## Responsabilidades

- Alta y baja de tokens FCM por usuario (`/api/v1/notifications/tokens`).
- Consumo del evento `alert.created` y despacho de la push correspondiente
  a los destinatarios de una alerta.
- Consumo del evento `user.sessions.revoked` y limpieza de tokens ante la
  revocacion de sesiones de un usuario.
- Deduplicacion de eventos por `eventId` para garantizar entrega efectiva
  una unica vez pese a la semantica at-least-once de RabbitMQ.

## Arquitectura

El servicio sigue Clean Architecture con los siguientes limites:

```
/cmd/api             punto de entrada: arranque de API y worker
/internal/domain     entidades e interfaces de dominio (sin dependencias externas)
/internal/service    casos de uso
/internal/repository capa de persistencia (Postgres via GORM)
/internal/delivery   adaptadores de entrada (HTTP y consumidores de RabbitMQ)
/internal/client     adaptadores de salida (Firebase Admin SDK)
/pkg/config          carga de configuracion desde variables de entorno
```

## Requisitos

- Go 1.26 o superior.
- Docker y Docker Compose para la ejecucion en contenedores.
- Red externa `vinculo-vial` ya creada (compartida con el resto de la
  plataforma) y broker RabbitMQ con la topologia declarada por
  `vinculo-vial-core` en ejecucion.

## Configuracion

Las variables de entorno requeridas estan documentadas en `.env.example`.
Para desarrollo local, copiar ese archivo a `.env` y completar los valores
marcados como `REPLACE_WITH_...`.

## Comandos

```
go run ./cmd/api
go test ./...
CGO_ENABLED=0 go build -o notification ./cmd/api
docker compose up --build
```

## Pruebas de integracion

`go test ./...` corre solo pruebas unitarias (SQLite y dobles en memoria).
La garantia central de idempotencia -dos entregas concurrentes del mismo
`event_id` producen un solo envio- depende del bloqueo que hace Postgres
sobre una fila sin confirmar, y se prueba contra un Postgres real, detras
del build tag `integration`:

```
docker run -d --name pg-it -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=notification_test -p 5432:5432 postgres:16-alpine
TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5432/notification_test?sslmode=disable" \
  go test -tags=integration -count=1 -run Integracion ./internal/repository/postgres/
```

En CI las corre un paso aparte del workflow, contra un servicio `postgres`.

## Salud del servicio

`GET /health` responde `200 OK` mientras el proceso este activo. Es una
sonda de liveness pura: no verifica dependencias externas.

`GET /ready` refleja el estado real de las dependencias (Postgres y ambas
conexiones a RabbitMQ). Devuelve `200 OK` con `{"status":"ready",...}`
cuando todas estan disponibles, o `503 Service Unavailable` con
`{"status":"not_ready",...}` y el detalle por dependencia en caso
contrario. Pensado como healthcheck pasivo para que Kong saque del
balanceo una instancia que sigue respondiendo HTTP pero no puede cumplir
su funcion (perdio la base o el broker).
