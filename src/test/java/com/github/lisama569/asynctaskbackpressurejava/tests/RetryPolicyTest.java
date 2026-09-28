package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.engine.RetryPolicy;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngineProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetryPolicyTest {

    @Test
    void backoffGrowsExponentiallyAndIsCapped() {
        TaskEngineProperties.Retry cfg = new TaskEngineProperties.Retry();
        cfg.setBackoff(Duration.ofMillis(50));
        cfg.setBackoffMultiplier(2.0);
        cfg.setMaxBackoff(Duration.ofMillis(400));
        cfg.setMaxAttempts(4);
        RetryPolicy policy = new RetryPolicy(cfg);

        assertEquals(4, policy.maxAttempts());
        assertEquals(50, policy.backoffForAttempt(1).toMillis());
        assertEquals(100, policy.backoffForAttempt(2).toMillis());
        assertEquals(200, policy.backoffForAttempt(3).toMillis());
        assertEquals(400, policy.backoffForAttempt(4).toMillis());
        assertEquals(400, policy.backoffForAttempt(10).toMillis());
    }

    @Test
    void zeroAttemptsConfigIsNormalizedToOne() {
        TaskEngineProperties.Retry cfg = new TaskEngineProperties.Retry();
        cfg.setMaxAttempts(0);
        assertEquals(1, new RetryPolicy(cfg).maxAttempts());
    }
}
