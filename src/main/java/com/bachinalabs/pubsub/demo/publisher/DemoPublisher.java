package com.bachinalabs.pubsub.demo.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DemoPublisher extends PubSubPublisher {

    private static Logger LOG = LoggerFactory.getLogger(DemoPublisher.class);

    @Value("${pubsub.topic}")
    private String topic;

    @Override
    protected String topic() {
        return this.topic;
    }
}
