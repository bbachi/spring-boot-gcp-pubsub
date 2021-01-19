package com.bachinalabs.pubsub.demo.publisher;

import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;

import java.util.concurrent.ExecutionException;

public abstract class PubSubPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubPublisher.class);

    @Autowired
    private PubSubTemplate pubSubTemplate;

    protected abstract String topic();

    public void publish(PubsubMessage pubsubMessage) throws ExecutionException, InterruptedException {
        LOG.info("Publishing to the topic [{}], message [{}]", topic(), pubsubMessage);
        pubSubTemplate.publish(topic(), pubsubMessage).get();
    }
}
