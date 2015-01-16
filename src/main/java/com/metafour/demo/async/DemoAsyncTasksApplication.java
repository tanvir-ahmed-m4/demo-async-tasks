package com.metafour.demo.async;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Demo application.
 * 
 * @author Tanvir
 *
 */
@Configuration
@ComponentScan
@EnableAutoConfiguration
public class DemoAsyncTasksApplication implements CommandLineRunner {

  private static final Logger logger = Logger.getLogger(DemoAsyncTasksApplication.class);

  @Autowired
  AnnotationConfigApplicationContext context;

  @Autowired
  AsyncScheduledProducer asp;

  @Value("${core.pool.size:3}")
  private int corePoolSize;
  @Value("${max.pool.size:5}")
  private int maxPoolSize;

  /**
   * A custom executor.
   * 
   * @return a {@link TaskExecutor}
   */
  @Bean(name = "AnExecutor")
  public TaskExecutor myExecutor() {
    ThreadPoolTaskExecutor myExecutor = new ThreadPoolTaskExecutor();
    myExecutor.setCorePoolSize(corePoolSize);
    myExecutor.setMaxPoolSize(maxPoolSize);
    myExecutor.setQueueCapacity(25);
    myExecutor.setThreadNamePrefix("AnExecutor-");
    myExecutor.afterPropertiesSet();
    myExecutor.setWaitForTasksToCompleteOnShutdown(true);
    return myExecutor;
  }

  public static void main(String[] args) {
    SpringApplication.run(DemoAsyncTasksApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    logger.info("Scheduler should start now...");
    logger.info("Waiting few seconds...");
    Thread.sleep(2000);

    logger.info("Starting async messages...");
    for (int i = 0; i < 10; i++) {
      asp.asyncMessage("Hello async " + i);
    }

    logger.info("Starting async processes that will return resutls...");
    AsyncResult ar = new AsyncResult(5);
    for (int i = 0; i < 5; i++) {
      asp.asyncTask(ar, i);
    }

    logger.info("Waiting on the async result latch...");
    ar.getLatch().await(1000, TimeUnit.MILLISECONDS);
    logger.info(String.format("*** Got async result from %d threds: %s", (5 - ar.getLatch()
        .getCount()), ar.getWords()));

    context.close();
  }

  @EnableAsync
  @EnableScheduling
  static class AsyncScheduledProducer {

    private final AtomicInteger counter = new AtomicInteger();

    @Scheduled(fixedRate = 1000)
    public void scheduledMessage() {
      logger.info("Scheduled message... " + counter.incrementAndGet());
    }

    @Async("AnExecutor")
    public void asyncMessage(String msg) throws InterruptedException {
      int wait = 500 + new Random().nextInt(500);
      Thread.sleep(wait);
      logger.info(String.format("Async message [%d]: %s ", wait, msg));
      Thread.sleep(500);
    }

    private String[] words =
        {"the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"};

    @Async
    public void asyncTask(AsyncResult ar, int id) throws Exception {
      int wait = 500 + new Random().nextInt(1000);
      logger.info(String.format("%d) Going to process for %d ms", id, wait));
      Thread.sleep(wait);
      if (wait % 2 == 0) {
        // we don't like even numbers
        throw new Exception(String.format("asyncTask %d Not happy!", id));
      }
      synchronized (ar) {
        ar.getWords().add(words[new Random().nextInt(words.length)]);
      }
      logger.info(String.format("%d) Adding results to AsyncResult@%s", id, ar.hashCode()));
      ar.getLatch().countDown();
    }
  }

  public class AsyncResult {

    private List<String> words;
    private CountDownLatch latch;

    public AsyncResult(int count) {
      words = new ArrayList<>();
      latch = new CountDownLatch(count);
    }

    public List<String> getWords() {
      return words;
    }

    public CountDownLatch getLatch() {
      return latch;
    }

  }

}
