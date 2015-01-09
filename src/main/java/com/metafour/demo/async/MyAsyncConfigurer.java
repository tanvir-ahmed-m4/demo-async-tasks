package com.metafour.demo.async;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

import org.apache.log4j.Logger;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * XXX: Why this executor service doesn't shut down?
 *  
 * @author Tanvir
 *
 */
@Configuration
@EnableAsync
public class MyAsyncConfigurer implements AsyncConfigurer {
	
	private static final Logger logger = Logger.getLogger(MyAsyncConfigurer.class);
	
	@Value("${use.default.executor:true}")
	private boolean useDefaultExecutor;
	
	@Value("${core.pool.size:5}")
	private int corePoolSize;
	@Value("${max.pool.size:10}")
	private int maxPoolSize;
	@Value("${queue.capacity:25}")
	private int queueCapacity;
	@Value("${thread.name.prefix:MyExecutor-}")
	private String threadNamePrefix;
	@Value("${wait.on.complete:true}")
	private boolean waitOnComplete;
	
	@Override
    public Executor getAsyncExecutor() {
		if (useDefaultExecutor) {
			return null;
		}
		
		// otherwise, create own executor
	    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
	    executor.setCorePoolSize(corePoolSize);
	    executor.setMaxPoolSize(maxPoolSize);
	    executor.setQueueCapacity(queueCapacity);
	    executor.setThreadNamePrefix(threadNamePrefix);
	    executor.setWaitForTasksToCompleteOnShutdown(waitOnComplete);
	    executor.setDaemon(true);
	    executor.afterPropertiesSet();
	    executor.initialize();
	    return executor;
	}
	
	@Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
		return new MyAsyncUncaughtExceptionHandler();
	}
	
	class MyAsyncUncaughtExceptionHandler implements AsyncUncaughtExceptionHandler {

        @Override
        public void handleUncaughtException(Throwable ex, Method method, Object... params) {
            logger.warn("Exception in async method: " + method.getName(), ex);
        }
    }
}
