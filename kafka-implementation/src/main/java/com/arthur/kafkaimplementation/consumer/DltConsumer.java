package com.arthur.kafkaimplementation.consumer;

import com.arthur.kafkaimplementation.dto.NotificationEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

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
@Component
public class DltConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltConsumer.class);

    @KafkaListener(
            topics = "${app.kafka.topic.dead-letter}",
            groupId = "notification-dlt-group"
    )
    public void consume(ConsumerRecord<String, NotificationEvent> record, Acknowledgment ack) {
        NotificationEvent event = record.value();

        log.warn("[DLT] Notificacao descartada por rate limit — userId={} type={} mensagem=\"{}\" sentAt={}",
                event.userId(), event.type(), event.message(), event.sentAt());

        // Aqui: persistir para retry posterior, emitir métrica, etc.

        ack.acknowledge();
    }
}
