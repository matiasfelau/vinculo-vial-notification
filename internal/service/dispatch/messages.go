package dispatch

import (
	"github.com/google/uuid"

	"github.com/matiasfelau/vinculo-vial-notification/internal/domain"
)

const (
	ownerTitle = "Alerta de Vinculo Vial"
	ownerBody  = "Tenes una nueva alerta. Abri la app para ver el detalle."

	issuerTitle = "ViVIAna"

	// issuerUpdateType es el valor de data.type con el que la app abre
	// el chat del emisor de la alerta indicada por data.alert_id.
	issuerUpdateType = "SENT_UPDATED"
)

// IssuerNotice identifica cual de los avisos al emisor corresponde.
// Cada uno mapea a un cambio de estado de la alerta enviada.
type IssuerNotice string

const (
	NoticeValidated IssuerNotice = "VALIDATED"
	NoticeSeen      IssuerNotice = "SEEN"
	NoticeRejected  IssuerNotice = "REJECTED"
	NoticeExpired   IssuerNotice = "EXPIRED"
)

var issuerBodies = map[IssuerNotice]string{
	NoticeValidated: "Tu alerta fue validada. Le avisé al dueño.",
	NoticeSeen:      "El dueño leyó tu mensaje.",
	NoticeRejected:  "No pudimos validar tu alerta.",
	NoticeExpired:   "Tu alerta no tuvo respuesta.",
}

// ownerMessage arma la push generica a los duenos. alert_id se suma solo
// si el evento lo trae (las alertas anteriores a US-09 no lo tienen):
// sin el, la app no tiene a donde enlazar y no se manda ese dato de deep
// link.
func ownerMessage(alertType string, alertID uuid.UUID) domain.PushMessage {
	data := map[string]string{"alert_type": alertType}
	if alertID != uuid.Nil {
		data["alert_id"] = alertID.String()
	}
	return domain.PushMessage{Title: ownerTitle, Body: ownerBody, Data: data}
}

// issuerMessage arma el aviso al emisor. El texto es generico: nunca
// lleva motivo de rechazo, veredicto ni dato del dueno. Sin alertID no
// hay deep link posible y se omite data.
func issuerMessage(notice IssuerNotice, alertID uuid.UUID) domain.PushMessage {
	message := domain.PushMessage{Title: issuerTitle, Body: issuerBodies[notice]}
	if alertID != uuid.Nil {
		message.Data = map[string]string{
			"type":     issuerUpdateType,
			"alert_id": alertID.String(),
		}
	}
	return message
}
