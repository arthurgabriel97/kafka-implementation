package com.arthur.kafkaimplementation.config;

import com.arthur.kafkaimplementation.dto.NotificationEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Cria os tópicos no Kafka na inicialização da aplicação e configura o producer.
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

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topic.notifications}")
    private String notificationsTopic;

    @Value("${app.kafka.topic.dead-letter}")
    private String deadLetterTopic;

    @Bean
    public ProducerFactory<String, NotificationEvent> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, NotificationEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

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
