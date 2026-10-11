package com.recker.fraudDetection.web;

import com.recker.fraudDetection.model.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    public TransactionController(KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @PostMapping("/sendTransaction")
    public String sendTransaction() throws Exception {
        for (int i = 0; i < 100 ; i++) {
            String transactionId = UUID.randomUUID().toString();
            double amount = 9000 + new Random().nextDouble() * (11000 - 8000);
            Transaction txn = new Transaction (transactionId, "USER_" + i,
                    amount, LocalDateTime.now().toString());

            kafkaTemplate.send("transactions", transactionId, txn);
        }
        return "Transaction sent successfully";
    }
}