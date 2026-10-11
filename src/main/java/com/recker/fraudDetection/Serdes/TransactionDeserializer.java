package com.recker.fraudDetection.Serdes;

import com.recker.fraudDetection.model.Transaction;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;


public class TransactionDeserializer implements Deserializer<Transaction> {

    ObjectMapper mapper = new ObjectMapper();

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        return mapper.readValue(data, Transaction.class);
    }
}
