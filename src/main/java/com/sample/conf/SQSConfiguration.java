package com.sample.conf;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.aws.core.region.RegionProvider;
import org.springframework.cloud.aws.core.region.StaticRegionProvider;
import org.springframework.cloud.aws.messaging.config.annotation.EnableSqs;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableSqs
@ConditionalOnProperty(name = {"MESSAGING_QUEUE", "AWS_ACCESS_KEY", "AWS_SECRET_KEY"})
public class SQSConfiguration {

  @Value("${MESSAGING_QUEUE_REGION:}")
  private String region;

  @Value("${MESSAGES_POLL_SIZE}")
  private String msgPollSize;

  @Bean
  @ConditionalOnProperty(name = {"MESSAGING_QUEUE_REGION"})
  public RegionProvider regionProvider() {
    return new StaticRegionProvider(region);
  }

}
