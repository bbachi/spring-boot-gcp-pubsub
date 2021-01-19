package com.bachinalabs.pubsub.demo.consumer;

import com.bachinalabs.pubsub.demo.publisher.DemoPublisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Component
public class DemoConsumer extends PubSubConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(DemoConsumer.class);

    @Autowired
    private PubSubTemplate pubSubTemplate;

    @Autowired
    private DemoPublisher demoPublisher;

    @Value("${pubsub.subscription}")
    private String subscription;

    @Override
    public String subscription() {
        return this.subscription;
    }

    @Override
    protected void consume(BasicAcknowledgeablePubsubMessage basicAcknowledgeablePubsubMessage) {

        PubsubMessage message = basicAcknowledgeablePubsubMessage.getPubsubMessage();

        try {
            System.out.println(message.getData().toStringUtf8());
            System.out.println(message.getAttributesMap());
            String objectName = message.getAttributesMap().get("objectId");
            String bucketName = message.getAttributesMap().get("bucketId");
            String eventType = message.getAttributesMap().get("eventType");

            LOG.info("Event Type:::::" + eventType);
            LOG.info("File Name::::::" + objectName);
            LOG.info("Bucket Name::::" + bucketName);

            String messageId = "messageId " + UUID.randomUUID();
            String pubMessage = "File Name Received " + objectName + "From Bucket " + bucketName + "For the event type::" + eventType;
            publishMessage(messageId, message.getAttributesMap(), pubMessage);
        }catch(Exception ex) {
            LOG.error("Error Occured while receiving pubsub message:::::", ex);
        }
        basicAcknowledgeablePubsubMessage.ack();
    }

    public void publishMessage(String messageId, Map<String, String> attributeMap, String message) throws ExecutionException, InterruptedException {
        LOG.info("Sending Message to the topic:::");
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .putAllAttributes(attributeMap)
                .setData(ByteString.copyFromUtf8(message))
                .setMessageId(messageId)
                .build();

        demoPublisher.publish(pubsubMessage);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void subscribe() {
        LOG.info("Subscribing {} to {} ", this.getClass().getSimpleName(), this.subscription());
        pubSubTemplate.subscribe(this.subscription(), this.consumer());
    }
}
