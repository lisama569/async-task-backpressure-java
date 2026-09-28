package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "task.engine.worker-threads=2",
        "task.engine.queue-capacity=4",
        "task.engine.default-bulkhead=2",
        "task.engine.reject-policy=ABORT",
        "task.engine.default-timeout=2s",
        "task.engine.default-deadline=10s",
        "task.engine.retry.max-attempts=1"
})
class TaskControllerIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ObjectMapper mapper;

    private final HttpClient http = HttpClient.newHttpClient();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpResponse<String> post(String path, Map<String, Object> body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url(path)))
                .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                .timeout(Duration.ofSeconds(10))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url(path)))
                .GET().timeout(Duration.ofSeconds(10)).build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private Map<String, Object> echoTask(String id, long sleepMs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("requestId", id);
        m.put("taskType", "echo");
        m.put("priority", "NORMAL");
        m.put("payload", "sleepMs=" + sleepMs);
        return m;
    }

    @Test
    void submitQueryAndCancelOverHttpWithClearErrorContracts() throws Exception {
        // 1) 提交一个会睡眠的 echo，立刻查询能看到中间态，最终 SUCCEEDED
        HttpResponse<String> created = post("/api/tasks", echoTask("http-1", 50));
        assertEquals(202, created.statusCode(), created.body());
        JsonNode createdBody = mapper.readTree(created.body());
        assertNotNull(createdBody.get("requestId"));

        HttpResponse<String> got = get("/api/tasks/http-1");
        assertEquals(200, got.statusCode());
        JsonNode verdict = awaitVerdict("http-1");
        assertEquals("SUCCEEDED", verdict.get("state").asText());
        assertEquals("echo:http-1", verdict.get("value").asText());

        // 2) 排队中取消：2 个长任务占住 worker，4 个短任务恰好填满队列
        for (int i = 0; i < 2; i++) {
            post("/api/tasks", echoTask("occupy-" + i, 800));
        }
        Thread.sleep(100); // 确保 occupy 已在执行，后续任务进入队列
        for (int i = 0; i < 4; i++) {
            HttpResponse<String> queued = post("/api/tasks", echoTask("q-" + i, 50));
            assertEquals(202, queued.statusCode(), "q-" + i + " should be queued: " + queued.body());
        }
        HttpResponse<String> cancel = post("/api/tasks/q-1/cancel", Map.of());
        assertEquals(200, cancel.statusCode(), cancel.body());
        assertEquals("CANCELLED", mapper.readTree(cancel.body()).get("state").asText());

        // 取消空出 1 个位置：补一个填满，再来一个必然 429
        HttpResponse<String> fill = post("/api/tasks", echoTask("fill-1", 10));
        assertEquals(202, fill.statusCode(), fill.body());

        // 3) 队列满 -> 429 + 结构化错误，不暴露线程池/队列内部异常类名
        HttpResponse<String> tooMany = post("/api/tasks", echoTask("overflow", 10));
        assertEquals(429, tooMany.statusCode(), tooMany.body());
        JsonNode err = mapper.readTree(tooMany.body());
        assertEquals("REJECTED", err.get("errorCode").asText());
        assertTrue(err.get("message").asText().contains("queue full"), tooMany.body());
        assertTrue(!tooMany.body().contains("RejectedExecutionException"));

        // 4) 非法请求 -> 400
        HttpResponse<String> bad = post("/api/tasks", Map.of("requestId", "x"));
        assertEquals(400, bad.statusCode());
        assertEquals("INVALID_TASK", mapper.readTree(bad.body()).get("errorCode").asText());

        // 5) 不存在任务 -> 404；统计可查
        assertEquals(404, get("/api/tasks/no-such-id").statusCode());
        HttpResponse<String> stats = get("/api/tasks/stats");
        assertEquals(200, stats.statusCode());
        JsonNode statsNode = mapper.readTree(stats.body());
        assertTrue(statsNode.get("queueCapacity").asInt() == 4);
        assertTrue(statsNode.has("types"));
    }

    private JsonNode awaitVerdict(String id) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            JsonNode node = mapper.readTree(get("/api/tasks/" + id).body());
            String state = node.get("state").asText();
            if (state.equals("SUCCEEDED") || state.equals("FAILED")
                    || state.equals("TIMEOUT") || state.equals("CANCELLED")
                    || state.equals("REJECTED")) {
                return node;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("no terminal verdict for " + id);
    }
}
