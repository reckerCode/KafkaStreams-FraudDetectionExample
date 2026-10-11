package com.recker.fraudDetection.stream;

import com.recker.fraudDetection.Serdes.TransactionSerdes;
import com.recker.fraudDetection.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
@Slf4j
public class FraudDetectionStream {

    @Bean
    public KStream<String, Transaction> getFraudDetectionStream(StreamsBuilder streamsBuilder) {
        // step 1: Read messages from the input topic.
        KStream<String, Transaction> transactionStream = streamsBuilder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerdes()));

        //step 2: Process the stream to detect fraudulent transactions.
        KStream<String, Transaction> fraudTransactionStream = transactionStream.filter((key, Value) -> isSuspicious(Value))
                .peek((key, value) -> {
                    log.warn("FRAUD ALERT - transactionId= {}, value= {}", key, value);
                });
        //Step 3: Write detected fraudulent transactions to an output topic
        fraudTransactionStream.to("fraud-alerts",  Produced.with(Serdes.String(), new TransactionSerdes()));
        return transactionStream;
    }

    private boolean isSuspicious(Transaction value) {
        try{
            return value.amount() > 10000;
        }catch (Exception e){
            return false;
        }
    }

}
