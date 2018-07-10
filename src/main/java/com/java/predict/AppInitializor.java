package com.java.predict;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.java.predict.service.ScoreRetrieverService;

/**
 * @author RuchitM
 * Initializor class to initialize application as spring boot app
 */
@EnableRetry
@SpringBootApplication
@PropertySource(value={"classpath:appConstant.properties"})
public class AppInitializor {

	@Autowired
	ScoreRetrieverService scoreRetrieverService;

	public static void main(String[] args) {
		SpringApplication.run(AppInitializor.class, args);
	}

	@PostConstruct
	public void registerInstance()
	{
		scoreRetrieverService.pollMessageFromQueue();
	}

	@Bean
	public RetryTemplate retryTemplate() {
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(3);
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(10000); // 1.5 seconds
		RetryTemplate template = new RetryTemplate();
		template.setRetryPolicy(retryPolicy);
		template.setBackOffPolicy(backOffPolicy);
		return template;
	}


}
