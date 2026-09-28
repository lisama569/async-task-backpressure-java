package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.asynctaskbackpressurejava.web.dto.SubmitTaskRequest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端 HTTP 测试：提交、状态查询、取消、指标，以及内部异常不外泄的错误码映射。
 * 使用启动的真实 Spring 容器与随机端口，仍完全本地、不依赖外部服务。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TaskControllerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(TaskControllerIntegrationTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private final ObjectMapper json = new ObjectMapper();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void submitQueryAndCompleteEchoTask() throws Exception {
        SubmitTaskRequest body = new SubmitTaskRequest();
        body.setRequestId("http-1");
        body.setTaskType("echo");
        body.setPayload("ping");

        ResponseEntity<String> resp = post(body);
        assertEquals(HttpStatus.ACCEPTED, resp.getStatusCode());
        log.info("submit http status={}, body={}", resp.getStatusCode().value(), resp.getBody());

        // 轮询直到终态
        JsonNode statusNode = null;
        for (int i = 0; i < 50; i++) {
            ResponseEntity<String> s = rest.getForEntity(url("/api/tasks/http-1"), String.class);
            assertEquals(HttpStatus.OK, s.getStatusCode());
            statusNode = json.readTree(s.getBody());
            if (!"QUEUED".equals(statusNode.path("status").path("status").asText())
                    && !"RUNNING".equals(statusNode.path("status").path("status").asText())) {
                break;
            }
            Thread.sleep(20);
        }
        log.info("final status body={}", statusNode);
        assertEquals("SUCCEEDED", statusNode.path("result").path("status").asText());
        assertEquals("echo:ping", statusNode.path("result").path("value").asText());
    }

    @Test
    void unknownTypeReturns400WithStableErrorCode() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("requestId", "http-bad");
        body.put("taskType", "does-not-exist");
        ResponseEntity<String> resp = postRaw(body);
        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertTrue(resp.getBody().contains("UNKNOWN_TASK_TYPE"));
        log.info("unknown type -> {}, body={}", resp.getStatusCode().value(), resp.getBody());
    }

    @Test
    void duplicateSubmitReturns409() {
        SubmitTaskRequest body = new SubmitTaskRequest();
        body.setRequestId("http-dup");
        body.setTaskType("echo");
        assertEquals(HttpStatus.ACCEPTED, post(body).getStatusCode());
        ResponseEntity<String> second = post(body);
        assertEquals(HttpStatus.CONFLICT, second.getStatusCode());
        assertTrue(second.getBody().contains("DUPLICATE_REQUEST"));
        log.info("duplicate -> {}, body={}", second.getStatusCode().value(), second.getBody());
    }

    @Test
    void metricsExposeQueueState() {
        ResponseEntity<String> resp = rest.getForEntity(url("/api/metrics"), String.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertTrue(resp.getBody().contains("perType"));
        log.info("metrics snippet: {}", resp.getBody().substring(0,
                Math.min(300, resp.getBody().length())));
    }

    @Test
    void cancelRunningSlowTask() throws Exception {
        SubmitTaskRequest body = new SubmitTaskRequest();
        body.setRequestId("http-cancel");
        body.setTaskType("slow");
        body.setPayload("sleep=10000");
        body.setTimeoutMillis(30_000L);
        assertEquals(HttpStatus.ACCEPTED, post(body).getStatusCode());

        // 等任务进入执行
        String state = "";
        for (int i = 0; i < 50; i++) {
            JsonNode node = json.readTree(rest.getForEntity(
                    url("/api/tasks/http-cancel"), String.class).getBody());
            state = node.path("status").path("status").asText();
            if ("RUNNING".equals(state)) {
                break;
            }
            Thread.sleep(20);
        }
        assertEquals("RUNNING", state);

        HttpHeaders headers = new HttpHeaders();
        ResponseEntity<String> cancel = rest.exchange(url("/api/tasks/http-cancel"),
                HttpMethod.DELETE, new HttpEntity<>(headers), String.class);
        assertEquals(HttpStatus.ACCEPTED, cancel.getStatusCode());

        JsonNode node = null;
        for (int i = 0; i < 50; i++) {
            node = json.readTree(rest.getForEntity(
                    url("/api/tasks/http-cancel"), String.class).getBody());
            if ("CANCELLED".equals(node.path("result").path("status").asText())) {
                break;
            }
            Thread.sleep(20);
        }
        log.info("after cancel body={}", node);
        assertEquals("CANCELLED", node.path("result").path("status").asText());
    }

    private ResponseEntity<String> post(SubmitTaskRequest body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return rest.postForEntity(url("/api/tasks"), new HttpEntity<>(body, headers), String.class);
    }

    private ResponseEntity<String> postRaw(Map<String, Object> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return rest.postForEntity(url("/api/tasks"), new HttpEntity<>(body, headers), String.class);
    }
}
