package com.recker.fraudDetection.model;

public record Transaction(
        String transactionId,
        String userId,
        double amount,
        String timestamp
) {
}
