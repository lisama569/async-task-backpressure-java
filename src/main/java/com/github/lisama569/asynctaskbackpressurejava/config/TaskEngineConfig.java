package com.github.lisama569.asynctaskbackpressurejava.config;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandlerRegistry;
import com.github.lisama569.asynctaskbackpressurejava.engine.DefaultTaskHandlerRegistry;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngine;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngineProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@EnableConfigurationProperties(TaskEngineProperties.class)
public class TaskEngineConfig {

    @Bean
    public TaskHandlerRegistry taskHandlerRegistry(List<TaskHandler> handlers) {
        DefaultTaskHandlerRegistry registry = new DefaultTaskHandlerRegistry();
        handlers.forEach(registry::register);
        return registry;
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public TaskEngine taskEngine(TaskEngineProperties properties,
                                 TaskHandlerRegistry registry) {
        return new TaskEngine(properties, registry);
    }
}
