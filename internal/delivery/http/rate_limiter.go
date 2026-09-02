package http

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	dailyTokenRegistrationLimit = 10
	rateLimitWindow             = 24 * time.Hour
)

// DailyRateLimiter limita la cantidad de altas de token (POST) por
// usuario en una ventana de 24 horas. El estado se mantiene en
// memoria: en un despliegue con multiples replicas cada instancia
// lleva su propio contador, lo cual es aceptable para este limite
// (no es un control de seguridad estricto, sino una salvaguarda
// contra uso abusivo del endpoint).
type DailyRateLimiter struct {
	mu      sync.Mutex
	buckets map[uuid.UUID]*rateLimitBucket
	now     func() time.Time
}

type rateLimitBucket struct {
	count       int
	windowStart time.Time
}

func NewDailyRateLimiter() *DailyRateLimiter {
	return &DailyRateLimiter{
		buckets: make(map[uuid.UUID]*rateLimitBucket),
		now:     time.Now,
	}
}

// Allow registra un intento para el owner dado y devuelve si esta
// dentro de la cuota diaria.
func (l *DailyRateLimiter) Allow(ownerID uuid.UUID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	bucket, exists := l.buckets[ownerID]
	if !exists || now.Sub(bucket.windowStart) >= rateLimitWindow {
		l.buckets[ownerID] = &rateLimitBucket{count: 1, windowStart: now}
		return true
	}

	if bucket.count >= dailyTokenRegistrationLimit {
		return false
	}

	bucket.count++
	return true
}

// Middleware aplica el limite diario. Se registra unicamente en el
// POST del endpoint de tokens: el DELETE nunca se limita.
func (l *DailyRateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID := ownerIDFromContext(c)

		if !l.Allow(ownerID) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "limite diario de registros de token alcanzado",
			})
			return
		}

		c.Next()
	}
}
