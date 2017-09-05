package com.sample.resources;

import java.util.UUID;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sqs.AmazonSQS;

@Component
@Produces(MediaType.APPLICATION_JSON)
@Path("/api")
public class RestResource {

  @Autowired
  private AmazonSQS amazonSQS;

  @Value("${MESSAGING_QUEUE:}")
  private String messagingQueue;

  @POST
  @Path("sqs")
  public void getUserProjects() {
    for (int i = 0; i < 15; i++) {
      amazonSQS.sendMessage(messagingQueue, "message from sqs listener " + UUID.randomUUID());
    }
  }
}
