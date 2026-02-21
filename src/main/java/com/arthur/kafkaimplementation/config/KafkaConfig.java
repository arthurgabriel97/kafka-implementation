package com.arthur.kafkaimplementation.config;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
@ApplicationScoped
public class KafkaConfig {

    @ConfigProperty(name = "kafka.bootstrap.servers", defaultValue = "localhost:9092")
    String bootstrapServers;

    @ConfigProperty(name = "app.kafka.topic.notifications", defaultValue = "notifications")
    String notificationsTopic;

    @ConfigProperty(name = "app.kafka.topic.dead-letter", defaultValue = "notifications.DLT")
    String deadLetterTopic;

    void onStart(@Observes StartupEvent event) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            List<NewTopic> topics = List.of(
                    new NewTopic(notificationsTopic, 3, (short) 1),
                    new NewTopic(deadLetterTopic, 1, (short) 1)
            );
            admin.createTopics(topics).all().get(10, TimeUnit.SECONDS);
            Log.infof("Tópicos Kafka criados: %s (3 partições), %s (1 partição)",
                    notificationsTopic, deadLetterTopic);
        } catch (ExecutionException e) {
            // org.apache.kafka.common.errors.TopicExistsException vem wrapped em ExecutionException
            Log.infof("Tópicos já existem, continuando: %s", e.getCause().getMessage());
        } catch (Exception e) {
            Log.warnf("Erro ao criar tópicos Kafka: %s", e.getMessage());
        }
    }
}
