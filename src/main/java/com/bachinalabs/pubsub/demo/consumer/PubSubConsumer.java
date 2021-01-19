package com.bachinalabs.pubsub.demo.consumer;

import com.google.cloud.pubsub.v1.Subscriber;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.support.BasicAcknowledgeablePubsubMessage;

import java.util.function.Consumer;

public abstract class PubSubConsumer {

    @Autowired
    private PubSubTemplate pubSubTemplate;

    /* Name of the Subscription */
    public abstract String subscription();

    protected abstract void consume(BasicAcknowledgeablePubsubMessage message);

    public Consumer<BasicAcknowledgeablePubsubMessage> consumer() {
        return basicAcknowledgeablePubsubMessage -> consume(basicAcknowledgeablePubsubMessage);
    }

    public Subscriber consumeMessage() {
        return this.pubSubTemplate.subscribe(this.subscription(), this.consumer());
    }
}
