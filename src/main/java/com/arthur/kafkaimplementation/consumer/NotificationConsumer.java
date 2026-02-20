package com.arthur.kafkaimplementation.consumer;

import com.arthur.kafkaimplementation.dto.NotificationEvent;
import com.arthur.kafkaimplementation.service.RateLimiterService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumer principal do tópico de notificações.
 *
 * Fluxo de processamento:
 *   1. Lê a mensagem do Kafka
 *   2. Consulta o RateLimiterService (sliding window no Redis)
 *   3a. Se bloqueado → publica no DLT e faz ACK (descarta da fila principal)
 *   3b. Se permitido → processa (simula envio) e faz ACK
 *
 * Por que ACK manual (enable-auto-commit: false + ack-mode: MANUAL)?
 *   O offset só é commitado após o ack explícito.
 *
 * EXPERIMENTO 1 — Matar o consumer no meio do processamento:
 *   Se o consumer morrer APÓS processar mas ANTES do ack.acknowledge(),
 *   o Kafka reentregará a mensagem (offset não foi commitado).
 *   Problema: o Redis JÁ foi incrementado. Na reentrega, o contador está +1
 *   do que deveria — risco de falso positivo no rate limit.
 *   Solução ideal: transação distribuída ou idempotency key (fora do escopo aqui).
 *
 * EXPERIMENTO 2 — Dois consumers no mesmo grupo:
 *   O Kafka divide as 3 partições entre os consumers:
 *   Consumer-A → partições [0, 1]
 *   Consumer-B → partição  [2]
 *   Só o consumer responsável pela partição de um userId específico
 *   vai processar as mensagens daquele usuário.
 */
@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final RateLimiterService rateLimiter;
    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private final String dltTopic;

    public NotificationConsumer(
            RateLimiterService rateLimiter,
            KafkaTemplate<String, NotificationEvent> kafkaTemplate,
            @Value("${app.kafka.topic.dead-letter}") String dltTopic
    ) {
        this.rateLimiter = rateLimiter;
        this.kafkaTemplate = kafkaTemplate;
        this.dltTopic = dltTopic;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.notifications}",
            groupId = "notification-group",
            concurrency = "1"
    )
    public void consume(ConsumerRecord<String, NotificationEvent> record, Acknowledgment ack) {
        NotificationEvent event = record.value();

        log.info("Mensagem recebida — userId={} type={} partition={} offset={}",
                event.userId(), event.type(), record.partition(), record.offset());

        if (!rateLimiter.isAllowed(event.userId())) {
            log.warn("BLOQUEADO pelo rate limit — enviando para DLT — userId={} type={}",
                    event.userId(), event.type());

            kafkaTemplate.send(dltTopic, event.userId(), event);

            // ACK mesmo no caso bloqueado: a mensagem foi tratada (enviada para DLT).
            // Sem o ACK aqui, o Kafka reentregaria indefinidamente após restart.
            ack.acknowledge();
            return;
        }

        processNotification(event);
        ack.acknowledge();
    }

    /**
     * Simula o envio da notificação via provedor externo (push, SMS, email).
     * Em produção, aqui entraria a chamada ao SDK do provedor (Firebase, Twilio, etc).
     */
    private void processNotification(NotificationEvent event) {
        log.info("ENVIANDO notificacao — userId={} type={} mensagem=\"{}\" sentAt={}",
                event.userId(), event.type(), event.message(), event.sentAt());

        // Simula latência de rede para o provedor externo
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("Notificacao ENTREGUE — userId={}", event.userId());
    }
}
