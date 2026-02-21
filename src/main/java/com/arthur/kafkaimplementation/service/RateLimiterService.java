package com.arthur.kafkaimplementation.service;

import io.quarkus.logging.Log;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.sortedset.ScoreRange;
import io.quarkus.redis.datasource.sortedset.SortedSetCommands;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.UUID;

/**
 * Rate limiter baseado em Sliding Window usando Redis Sorted Sets.
 *
 * Estrutura no Redis:
 *   Key:   rate_limit:{userId}
 *   Type:  Sorted Set (ZSet)
 *   Score: timestamp em milissegundos do momento em que a notificação foi processada
 *   Value: UUID único por entrada (evita colisões de score)
 *
 * Algoritmo (isAllowed):
 *   1. ZREMRANGEBYSCORE → remove entradas mais antigas que (agora - janela)
 *   2. ZCARD            → conta quantas entradas restam na janela
 *   3. Se count >= limite → bloqueado (retorna false)
 *   4. ZADD             → registra o processamento atual
 *   5. EXPIRE           → mantém o TTL do key alinhado à janela
 *
 * EXPERIMENTO — Zerando o Redis com consumer rodando:
 *   redis-cli FLUSHALL
 *   → O sorted set some. O rate limit volta do zero para todos os usuários.
 *   → Todos voltam a receber notificações sem restrição até o contador se reconstituir.
 *   → Isso demonstra que o estado do rate limiter é volátil e Redis é um ponto de falha.
 */
@ApplicationScoped
public class RateLimiterService {

    private static final String KEY_PREFIX = "rate_limit:";

    private final SortedSetCommands<String, String> sortedSet;
    private final RedisDataSource redisDataSource;
    private final int maxPerMinute;
    private final int windowSeconds;

    public RateLimiterService(
            RedisDataSource redisDataSource,
            @ConfigProperty(name = "app.rate-limit.max-per-minute", defaultValue = "5") int maxPerMinute,
            @ConfigProperty(name = "app.rate-limit.window-seconds", defaultValue = "60") int windowSeconds
    ) {
        this.redisDataSource = redisDataSource;
        this.sortedSet = redisDataSource.sortedSet(String.class);
        this.maxPerMinute = maxPerMinute;
        this.windowSeconds = windowSeconds;
    }

    /**
     * Verifica se o usuário ainda está dentro do limite e registra a tentativa.
     *
     * @param userId identificador do usuário
     * @return true se pode processar, false se deve ser descartado/enviado para DLT
     */
    public boolean isAllowed(String userId) {
        String key = KEY_PREFIX + userId;
        long nowMs = System.currentTimeMillis();
        long windowStartMs = nowMs - (windowSeconds * 1000L);

        // Remove entradas fora da janela deslizante
        sortedSet.zremrangebyscore(key, new ScoreRange<>(0.0, (double) windowStartMs));

        // Conta quantas notificações foram processadas na janela atual
        long currentCount = sortedSet.zcard(key);

        Log.debugf("Rate limit check — userId=%s count=%d/%d window=%ds",
                userId, currentCount, maxPerMinute, windowSeconds);

        if (currentCount >= maxPerMinute) {
            Log.warnf("Rate limit EXCEDIDO — userId=%s (%d notificações na última janela de %ds)",
                    userId, currentCount, windowSeconds);
            return false;
        }

        // Registra este processamento com timestamp como score
        sortedSet.zadd(key, (double) nowMs, UUID.randomUUID().toString());

        // Mantém TTL para que chaves inativas não acumulem no Redis
        redisDataSource.key().expire(key, Duration.ofSeconds(windowSeconds + 5));

        return true;
    }

    /**
     * Retorna quantas notificações o usuário processou na janela atual.
     * Útil para endpoints de observabilidade.
     */
    public long getCount(String userId) {
        String key = KEY_PREFIX + userId;
        long windowStartMs = System.currentTimeMillis() - (windowSeconds * 1000L);
        sortedSet.zremrangebyscore(key, new ScoreRange<>(0.0, (double) windowStartMs));
        return sortedSet.zcard(key);
    }
}
