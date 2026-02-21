package com.arthur.kafkaimplementation.consumer;

import com.arthur.kafkaimplementation.dto.NotificationEvent;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletionStage;

/**
 * Consumer do Dead Letter Topic (DLT).
 *
 * Recebe notificações que foram bloqueadas pelo rate limiter.
 * Em produção, aqui você poderia:
 *   - Salvar em banco para auditoria
 *   - Agendar reenvio depois da janela do rate limit
 *   - Emitir métricas (Prometheus/Datadog) sobre notificações descartadas
 *   - Notificar o time via alertas (PagerDuty, Slack)
 *
 * Grupo separado "notification-dlt-group" → independente do consumer principal.
 * Isso significa que parar o consumer principal não para o DLT consumer e vice-versa.
 */
@ApplicationScoped
public class DltConsumer {

    @Incoming("notifications-dlt-in")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @Blocking
    public CompletionStage<Void> consume(Message<NotificationEvent> message) {
        NotificationEvent event = message.getPayload();

        Log.warnf("[DLT] Notificacao descartada por rate limit — userId=%s type=%s mensagem=\"%s\" sentAt=%s",
                event.userId(), event.type(), event.message(), event.sentAt());

        // Aqui: persistir para retry posterior, emitir métrica, etc.

        return message.ack();
    }
}
