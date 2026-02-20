package com.arthur.kafkaimplementation.dto;

import java.time.Instant;

/**
 * Evento de notificação que trafega pelo Kafka.
 *
 * userId  → chave de particionamento (mesmo usuário → mesma partição → ordem garantida)
 * type    → PROMOCAO | PEDIDO | ESTOQUE
 * message → conteúdo da notificação
 * sentAt  → timestamp de criação (usado para debugging)
 */
public record NotificationEvent(
        String userId,
        String type,
        String message,
        Instant sentAt
) {}
