package com.sample.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.stereotype.Component;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

@Component
@Configuration
public class MessageAdapter {

  private static final Logger logger = LoggerFactory.getLogger(MessageAdapter.class);
  private static final String ALL_ATTRIBUTES = "All";
  private ReceiveMessageRequest receiveMessageRequest;
  private Integer visibilityTimeOutInSeconds;

  @Autowired
  private AmazonSQS amazonSQS;

  @Value("${MESSAGING_QUEUE:}")
  private String messagingQueue;

  @Value("${MY_POLLER_FIXED_DELAY:86400000}")
  private long pollerFixedDelay;

  @Value("${MY_TIME_BOUND:60000}")
  private long timeBound;

  @Value("${MY__VISIBILITY_TIMEOUT:300000}")
  private long visibilityTimeout;

  @PostConstruct
  public void init() {
    receiveMessageRequest =
        new ReceiveMessageRequest(messagingQueue).withMessageAttributeNames(ALL_ATTRIBUTES)
            .withAttributeNames(ALL_ATTRIBUTES).withMaxNumberOfMessages(Integer.valueOf(10));

    visibilityTimeOutInSeconds =
        Integer.valueOf(Math.toIntExact(TimeUnit.MILLISECONDS.toSeconds(visibilityTimeout)));

    logger.info(String.format(
        "Start DLQMessagingAdapter component with timeBound: %s, dlqVisibilityTimeout: %s, "
            + "visibilityTimeOutInSeconds: %s, pollerFixedDelay: %s",
        Long.valueOf(timeBound), Long.valueOf(visibilityTimeout), visibilityTimeOutInSeconds,
        Long.valueOf(pollerFixedDelay)));
  }

  @InboundChannelAdapter(value = "pollingChannel",
      poller = @Poller(fixedDelay = "60000", maxMessagesPerPoll = "10"))
  public List<Message> receiveMessagesFromDLQ() {
    List<Message> messageList = new ArrayList<>();
    try {
      messageList = amazonSQS.receiveMessage(receiveMessageRequest).getMessages();
    } catch (Exception e) {
      logger.error(
          String.format("Error occurred while receiving messages polled from: %s", messagingQueue),
          e);
    }
    return messageList;
  }

  @ServiceActivator(inputChannel = "pollingChannel")
  public void processMessages(List<Message> messages) {
    if (!messages.isEmpty()) {
      changeMessageVisibilityBatch(messages);
    }
  }

  @Bean
  public MessageChannel pollingChannel() {
    return new QueueChannel(500);
  }

  @Bean(name = PollerMetadata.DEFAULT_POLLER)
  public PollerMetadata poller() {
    PeriodicTrigger trigger = new PeriodicTrigger(10);
    trigger.setFixedRate(true);
    PollerMetadata pollerMetadata = new PollerMetadata();
    pollerMetadata.setTrigger(trigger);
    return pollerMetadata;
  }

  private void changeMessageVisibilityBatch(List<Message> messages) {
    try {
      int requestId = 0;
      List<ChangeMessageVisibilityBatchRequestEntry> entries = new ArrayList<>(messages.size());
      for (Message message : messages) {
        entries.add(new ChangeMessageVisibilityBatchRequestEntry()
            .withId(Integer.toString(requestId++)).withReceiptHandle(message.getReceiptHandle())
            .withVisibilityTimeout(visibilityTimeOutInSeconds));
      }

      ChangeMessageVisibilityBatchRequest batchRequest =
          new ChangeMessageVisibilityBatchRequest().withQueueUrl(messagingQueue);
      batchRequest.setEntries(entries);
      ChangeMessageVisibilityBatchResult changeMessageVisibilityBatchResult =
          amazonSQS.changeMessageVisibilityBatch(batchRequest);

      // Retry failing request
      if (!changeMessageVisibilityBatchResult.getFailed().isEmpty()) {
        changeMessageVisibilityBatchResult.getFailed().forEach(m -> {
          int index = Integer.parseInt(m.getId());
          ChangeMessageVisibilityBatchRequestEntry entry = entries.get(index);
          amazonSQS.changeMessageVisibility(messagingQueue, entry.getReceiptHandle(),
              entry.getVisibilityTimeout());
          logger.info(String.format("Second attempt to change visibility successfully for entry %s",
              entry));
        });
      }
      logger
          .info(String.format("Batch change visibility to %s seconds successfully for messages %s",
              visibilityTimeOutInSeconds, messages));
    } catch (AmazonClientException ace) {
      logger.error("Error occurred during the message visibility change process", ace);
    }
  }
}
