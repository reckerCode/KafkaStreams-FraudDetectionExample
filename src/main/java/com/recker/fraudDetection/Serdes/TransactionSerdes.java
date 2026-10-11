package com.recker.fraudDetection.Serdes;

import com.recker.fraudDetection.model.Transaction;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransactionSerdes extends Serdes.WrapperSerde<Transaction>{

    public TransactionSerdes(){
        super(new TransactionSerializer(),new TransactionDeserializer());
    }

}
