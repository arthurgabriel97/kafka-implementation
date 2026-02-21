# Kafka Implementation

Aplicação de demonstração de padrões enterprise com **Apache Kafka**, desenvolvida com **Quarkus**, **Redis** e **Docker**. Implementa um sistema de notificações com rate limiting por janela deslizante, Dead Letter Topic (DLT) e particionamento por usuário.

## Visão Geral

A aplicação simula um sistema de envio de notificações onde cada usuário pode receber no máximo **5 notificações por minuto**. Mensagens que excedem o limite são automaticamente encaminhadas para um Dead Letter Topic para auditoria e possível reprocessamento.

### Fluxo da Aplicação

```
Cliente HTTP
    │
    ▼
NotificationController  (REST API)
    │
    ▼
NotificationProducer    (publica no Kafka com userId como chave)
    │
    ▼
Kafka Topic: notifications  (3 partições)
    │
    ▼
NotificationConsumer    (consome e verifica rate limit via Redis)
    │
    ├─── PERMITIDA ──► processNotification() ──► log de entrega
    │
    └─── BLOQUEADA ──► DLT Emitter ──► Kafka Topic: notifications.DLT
                                              │
                                              ▼
                                         DltConsumer  (loga mensagens descartadas)
```

## Tecnologias

| Tecnologia | Versão | Uso |
|---|---|---|
| Java | 21 | Linguagem |
| Quarkus | 3.15.1 | Framework principal |
| Apache Kafka | 4.1.1 | Mensageria |
| SmallRye Reactive Messaging | — | Integração Kafka/Quarkus |
| Redis | 7.2 | Rate limiting (Sorted Sets) |
| Jackson | — | Serialização/Deserialização JSON |
| Docker Compose | — | Infraestrutura local |
| Maven | 3.9.12 | Build |

## Pré-requisitos

- Java 21+
- Docker e Docker Compose
- Maven 3.9+ (ou use o `./mvnw` incluso)

## Executando a Aplicação

### 1. Subir a infraestrutura (Kafka + Redis)

```bash
docker compose up -d
```

Isso inicializa:
- **Kafka 4.1.1** em `localhost:9092` (modo KRaft, sem ZooKeeper)
- **Redis 7.2** em `localhost:6379` (com persistência habilitada)

### 2. Iniciar a aplicação

```bash
./mvnw quarkus:dev
```

A aplicação estará disponível em `http://localhost:8080`.

## API REST

### Enviar uma notificação

```bash
POST /api/notifications
Content-Type: application/json

{
  "userId": "u1",
  "type": "PEDIDO",
  "message": "Seu pedido foi confirmado!"
}
```

**Resposta:** `202 Accepted`

### Enviar múltiplas notificações (teste de rate limit)

```bash
POST /api/notifications/burst?userId=u1&count=8&type=PROMOCAO
```

Envia `count` notificações em sequência para o mesmo usuário. As primeiras 5 são entregues normalmente; as demais são enviadas ao DLT.

### Consultar status de rate limit de um usuário

```bash
GET /api/notifications/rate-limit/{userId}
```

**Resposta:**
```json
{
  "userId": "u1",
  "count": 3,
  "limit": 5,
  "blocked": false
}
```

## Modelo de Dados

```java
public record NotificationEvent(
    String userId,    // Identificador do usuário (chave de partição)
    String type,      // PROMOCAO | PEDIDO | ESTOQUE
    String message,   // Conteúdo da notificação
    Instant sentAt    // Timestamp de criação
) {}
```

## Conceitos e Decisões de Design

### Particionamento por `userId`

As mensagens são publicadas no Kafka usando `userId` como chave. Isso garante que:
- Todas as notificações de um mesmo usuário vão para a mesma partição
- A ordem de entrega por usuário é preservada
- Diferentes usuários são processados em paralelo

### Rate Limiting com Redis (Janela Deslizante)

O `RateLimiterService` utiliza **Redis Sorted Sets** para implementar o algoritmo de janela deslizante:

1. Remove entradas com timestamp fora da janela atual (`ZREMRANGEBYSCORE`)
2. Conta as entradas restantes (`ZCARD`)
3. Se `count >= maxPerMinute` → mensagem **bloqueada**
4. Caso contrário, adiciona nova entrada com timestamp atual (`ZADD`)
5. Define expiração da chave (`EXPIRE`) para limpeza automática

**Configuração padrão:** 5 mensagens por minuto (ajustável em `application.properties`)

### Dead Letter Topic (DLT)

Mensagens bloqueadas pelo rate limiter são encaminhadas para o tópico `notifications.DLT`. O `DltConsumer` processa esse tópico de forma independente, permitindo:
- Auditoria das mensagens rejeitadas
- Implementação de estratégias de retry

### Commit Manual de Offsets

O auto-commit está desabilitado (`enable.auto.commit=false`). O offset só é confirmado após o processamento explícito da mensagem (`message.ack()`), garantindo semântica de **entrega pelo menos uma vez** (*at-least-once delivery*).

## Configuração

As principais configurações ficam em `src/main/resources/application.properties`:

```properties
# Rate Limiting
app.rate-limit.max-per-minute=5
app.rate-limit.window-seconds=60

# Kafka
kafka.bootstrap.servers=localhost:9092

# Redis
quarkus.redis.hosts=redis://localhost:6379

# Tópicos
app.kafka.topic.notifications=notifications
app.kafka.topic.dead-letter=notifications.DLT
```

## Estrutura do Projeto

```
src/
└── main/
    └── java/com/arthur/kafkaimplementation/
        ├── KafkaImplementationApplication.java   # Ponto de entrada
        ├── config/
        │   └── KafkaConfig.java                 # Criação dos tópicos na inicialização
        ├── controller/
        │   └── NotificationController.java       # Endpoints REST
        ├── producer/
        │   └── NotificationProducer.java         # Publicação no Kafka
        ├── consumer/
        │   ├── NotificationConsumer.java          # Consumidor principal + rate limit
        │   └── DltConsumer.java                  # Consumidor do Dead Letter Topic
        ├── service/
        │   └── RateLimiterService.java           # Rate limiting com Redis
        └── dto/
            ├── NotificationEvent.java            # Modelo de dados
            ├── NotificationEventSerializer.java  # Serializador Jackson/Kafka
            └── NotificationEventDeserializer.java # Deserializador Jackson/Kafka
```

## Experimentos Sugeridos

### 1. Teste de Rate Limit

```bash
# Envia 8 notificações; as 3 últimas vão para o DLT
POST /api/notifications/burst?userId=u1&count=8&type=PROMOCAO
```

Observe nos logs: as primeiras 5 com `[ENTREGUE]` e as demais com `[DLT] Mensagem descartada`.

### 2. Falha do Consumidor

Interrompa a aplicação no meio do processamento de uma mensagem e reinicie. Verifique que as mensagens não confirmadas são reprocessadas (offset não foi commitado).

### 3. Distribuição de Partições

Inicie múltiplas instâncias da aplicação e envie notificações com diferentes `userId`. Observe como o Kafka distribui as partições entre os consumidores do mesmo grupo.

### 4. Volatilidade do Redis

Limpe o Redis enquanto a aplicação está rodando:

```bash
docker exec -it <redis-container> redis-cli FLUSHALL
```

Observe que o rate limit é reiniciado para todos os usuários.

## Executando os Testes

```bash
./mvnw test
```

> **Nota:** Os testes de integração requerem Kafka e Redis disponíveis (via Docker Compose).

## Parando a Infraestrutura

```bash
docker compose down
```

Para remover também os volumes (dados persistidos):

```bash
docker compose down -v
```
