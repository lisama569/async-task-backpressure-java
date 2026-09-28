package com.github.lisama569.asynctaskbackpressurejava.config;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskEngine;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.demo.DemoHandlers;
import com.github.lisama569.asynctaskbackpressurejava.engine.LocalTaskEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring 装配：按 task-engine.types 配置注册每条类型流水线并启动引擎；
 * 容器关闭时由 destroyMethod="shutdown" 统一回收线程（无资源泄漏）。
 */
@Configuration
@EnableConfigurationProperties(TaskEngineProperties.class)
public class TaskEngineConfig {

    private static final Logger log = LoggerFactory.getLogger(TaskEngineConfig.class);

    private final TaskEngineProperties properties;
    private final DemoHandlers demoHandlers;

    public TaskEngineConfig(TaskEngineProperties properties, DemoHandlers demoHandlers) {
        this.properties = properties;
        this.demoHandlers = demoHandlers;
    }

    @Bean(destroyMethod = "shutdown")
    public TaskEngine taskEngine() {
        LocalTaskEngine engine = new LocalTaskEngine(
                properties.getAgingThresholdMillis(),
                properties.getResultStoreMaxEntries(),
                properties.getResultTtlMillis());
        registerType(engine, "echo", demoHandlers.echo(), 200, 4);
        registerType(engine, "slow", demoHandlers.slow(), 100, 2);
        registerType(engine, "flaky", demoHandlers.flaky(), 100, 2);
        registerType(engine, "alwaysFail", demoHandlers.alwaysFail(), 100, 2);
        if (properties.isEnabled()) {
            engine.start();
        } else {
            log.info("task engine disabled by configuration");
        }
        return engine;
    }

    private void registerType(LocalTaskEngine engine, String name, TaskHandler handler,
                              int defaultCapacity, int defaultWorkers) {
        TaskEngineProperties.TypeConfig cfg = properties.getTypes().get(name);
        int capacity = cfg == null ? defaultCapacity : cfg.getQueueCapacity();
        int workers = cfg == null ? defaultWorkers : cfg.getWorkers();
        engine.register(name, handler, capacity, workers);
    }
}
