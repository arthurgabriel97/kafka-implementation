package com.arthur.kafkaimplementation.producer;

import com.arthur.kafkaimplementation.dto.NotificationEvent;
import io.quarkus.logging.Log;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Producer de notificações para o Kafka.
 *
 * Decisão de design — userId como chave da mensagem:
 *   O Kafka usa a chave para determinar em qual partição a mensagem vai.
 *   Usar userId como chave garante que todas as notificações do mesmo usuário
 *   caiam na mesma partição, preservando a ordem de processamento por usuário.
 *
 *   Sem chave → round-robin entre partições → notificações do mesmo usuário
 *               podem ser processadas fora de ordem por consumers diferentes.
 */
@ApplicationScoped
public class NotificationProducer {

    @Inject
    @Channel("notifications-out")
    Emitter<NotificationEvent> emitter;

    public void send(NotificationEvent event) {
        var metadata = OutgoingKafkaRecordMetadata.<String>builder()
                .withKey(event.userId())
                .build();

        emitter.send(Message.of(event).addMetadata(metadata))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        Log.errorf("Falha ao publicar notificação — userId=%s error=%s",
                                event.userId(), ex.getMessage());
                    } else {
                        Log.infof("Notificação publicada — userId=%s type=%s",
                                event.userId(), event.type());
                    }
                });
    }
}
