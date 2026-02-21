package com.arthur.kafkaimplementation.consumer;

import com.arthur.kafkaimplementation.dto.NotificationEvent;
import com.arthur.kafkaimplementation.service.RateLimiterService;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletionStage;

/**
 * Consumer principal do tópico de notificações.
 *
 * Fluxo de processamento:
 *   1. Lê a mensagem do Kafka
 *   2. Consulta o RateLimiterService (sliding window no Redis)
 *   3a. Se bloqueado → publica no DLT e faz ACK (descarta da fila principal)
 *   3b. Se permitido → processa (simula envio) e faz ACK
 *
 * Por que ACK manual (enable-auto-commit: false + Strategy.MANUAL)?
 *   O offset só é commitado após o ack explícito.
 *
 * EXPERIMENTO 1 — Matar o consumer no meio do processamento:
 *   Se o consumer morrer APÓS processar mas ANTES do message.ack(),
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
@ApplicationScoped
public class NotificationConsumer {

    @Inject
    RateLimiterService rateLimiter;

    @Inject
    @Channel("notifications-dlt-out")
    Emitter<NotificationEvent> dltEmitter;

    @Incoming("notifications-in")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @Blocking
    public CompletionStage<Void> consume(Message<NotificationEvent> message) {
        NotificationEvent event = message.getPayload();

        IncomingKafkaRecordMetadata<?, ?> meta =
                message.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();

        Log.infof("Mensagem recebida — userId=%s type=%s partition=%d offset=%d",
                event.userId(), event.type(), meta.getPartition(), meta.getOffset());

        if (!rateLimiter.isAllowed(event.userId())) {
            Log.warnf("BLOQUEADO pelo rate limit — enviando para DLT — userId=%s type=%s",
                    event.userId(), event.type());

            dltEmitter.send(Message.of(event).addMetadata(
                    OutgoingKafkaRecordMetadata.<String>builder()
                            .withKey(event.userId())
                            .build()
            ));

            // ACK mesmo no caso bloqueado: a mensagem foi tratada (enviada para DLT).
            // Sem o ACK aqui, o Kafka reentregaria indefinidamente após restart.
            return message.ack();
        }

        processNotification(event);
        return message.ack();
    }

    /**
     * Simula o envio da notificação via provedor externo (push, SMS, email).
     * Em produção, aqui entraria a chamada ao SDK do provedor (Firebase, Twilio, etc).
     */
    private void processNotification(NotificationEvent event) {
        Log.infof("ENVIANDO notificacao — userId=%s type=%s mensagem=\"%s\" sentAt=%s",
                event.userId(), event.type(), event.message(), event.sentAt());

        // Simula latência de rede para o provedor externo
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Log.infof("Notificacao ENTREGUE — userId=%s", event.userId());
    }
}
