package com.arthur.kafkaimplementation.controller;

import com.arthur.kafkaimplementation.dto.NotificationEvent;
import com.arthur.kafkaimplementation.producer.NotificationProducer;
import com.arthur.kafkaimplementation.service.RateLimiterService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.Instant;
import java.util.Map;

/**
 * API REST para disparar notificações manualmente.
 * Usada para testar o comportamento do rate limiter sem precisar de um sistema externo.
 *
 * Endpoints:
 *
 *   POST /api/notifications
 *     → Envia uma notificação única
 *     → Body: { "userId": "u1", "type": "PEDIDO", "message": "Seu pedido foi enviado!" }
 *
 *   POST /api/notifications/burst?userId=u1&count=8
 *     → Envia N notificações para o mesmo usuário rapidamente
 *     → Demonstra o rate limiter em ação: as primeiras 5 passam, o resto vai para DLT
 *
 *   GET /api/notifications/rate-limit/{userId}
 *     → Consulta quantas notificações o usuário processou na janela atual
 */
@Path("/api/notifications")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NotificationController {

    @Inject
    NotificationProducer producer;

    @Inject
    RateLimiterService rateLimiter;

    @POST
    public Response send(NotificationRequest request) {
        NotificationEvent event = new NotificationEvent(
                request.userId(),
                request.type(),
                request.message(),
                Instant.now()
        );

        producer.send(event);

        return Response.accepted(Map.of(
                "status", "publicado",
                "userId", event.userId(),
                "type", event.type(),
                "info", "O rate limit é verificado no consumer, não aqui."
        )).build();
    }

    /**
     * Envia N notificações seguidas para o mesmo usuário.
     * O producer publica todas no Kafka imediatamente.
     * O consumer vai processar e aplicar o rate limit — as primeiras 5
     * passam, o restante é redirecionado para o DLT.
     *
     * Exemplo: POST /api/notifications/burst?userId=usuario-1&count=8&type=PROMOCAO
     */
    @POST
    @Path("/burst")
    public Response burst(
            @QueryParam("userId") String userId,
            @QueryParam("count") @DefaultValue("8") int count,
            @QueryParam("type") @DefaultValue("PROMOCAO") String type
    ) {
        for (int i = 1; i <= count; i++) {
            NotificationEvent event = new NotificationEvent(
                    userId,
                    type,
                    "Notificacao #" + i + " — " + type + " para usuario " + userId,
                    Instant.now()
            );
            producer.send(event);
        }

        return Response.accepted(Map.of(
                "userId", userId,
                "enviadas_ao_kafka", count,
                "limite_por_minuto", 5,
                "expectativa", "Primeiras 5 processadas, demais vao para DLT"
        )).build();
    }

    @GET
    @Path("/rate-limit/{userId}")
    public Response getRateLimit(@PathParam("userId") String userId) {
        long count = rateLimiter.getCount(userId);

        return Response.ok(Map.of(
                "userId", userId,
                "notificacoes_na_janela", count,
                "limite", 5,
                "bloqueado", count >= 5
        )).build();
    }

    public record NotificationRequest(String userId, String type, String message) {}
}
