package com.recker.fraudDetection.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    public NewTopic createTransactionTopic() {
        return new NewTopic("transactions", 3, (short) 1);
    }
}
