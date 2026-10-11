package com.recker.fraudDetection.Serdes;

import com.recker.fraudDetection.model.Transaction;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class TransactionSerializer implements Serializer<Transaction> {
    private final ObjectMapper mapper = new ObjectMapper();


    @Override
    public byte[] serialize(String topic, Transaction data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing Transaction", e);
        }
    }

}
