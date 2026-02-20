package com.arthur.kafkaimplementation.producer;

import com.arthur.kafkaimplementation.dto.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

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
@Service
public class NotificationProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationProducer.class);

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private final String topic;

    public NotificationProducer(
            KafkaTemplate<String, NotificationEvent> kafkaTemplate,
            @Value("${app.kafka.topic.notifications}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(NotificationEvent event) {
        CompletableFuture<SendResult<String, NotificationEvent>> future =
                kafkaTemplate.send(topic, event.userId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Falha ao publicar notificação — userId={} error={}",
                        event.userId(), ex.getMessage());
            } else {
                log.info("Notificação publicada — userId={} type={} partition={} offset={}",
                        event.userId(),
                        event.type(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
