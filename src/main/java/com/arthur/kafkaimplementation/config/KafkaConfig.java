package com.arthur.kafkaimplementation.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Cria os tópicos no Kafka na inicialização da aplicação.
 *
 * notifications  → tópico principal com 3 partições
 *                  (3 partições permitem testar divisão entre consumers no mesmo group)
 *
 * notifications.DLT → Dead Letter Topic para notificações bloqueadas pelo rate limiter
 *                      (1 partição é suficiente para DLT)
 *
 * EXPERIMENTO: Sobe dois consumers no mesmo consumer group "notification-group"
 * e observe no log qual consumer processa cada partição. O Kafka vai dividir as
 * 3 partições entre eles (ex: consumer-1 pega 0,1 e consumer-2 pega 2).
 */
@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic.notifications}")
    private String notificationsTopic;

    @Value("${app.kafka.topic.dead-letter}")
    private String deadLetterTopic;

    @Bean
    public NewTopic notificationsTopic() {
        return TopicBuilder.name(notificationsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(deadLetterTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
