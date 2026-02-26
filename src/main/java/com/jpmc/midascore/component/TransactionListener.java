package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRecordRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class TransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    
    @Autowired
    private UserRecordRepository userRecordRepository;
    
    @Autowired
    private TransactionRecordRepository transactionRecordRepository;
    
    @Autowired
    private RestTemplate restTemplate;
    
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Transactional
    public void listen(@Payload Transaction transaction) {
        logger.info("Received transaction: senderId={}, recipientId={}, amount={}", 
            transaction.getSenderId(), 
            transaction.getRecipientId(), 
            transaction.getAmount());
        
        // Validate and process transaction
        Optional<UserRecord> senderOpt = userRecordRepository.findById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = userRecordRepository.findById(transaction.getRecipientId());
        
        if (senderOpt.isEmpty()) {
            logger.warn("Invalid sender ID: {}", transaction.getSenderId());
            return;
        }
        
        if (recipientOpt.isEmpty()) {
            logger.warn("Invalid recipient ID: {}", transaction.getRecipientId());
            return;
        }
        
        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();
        
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender {}: balance={}, amount={}", 
                sender.getId(), sender.getBalance(), transaction.getAmount());
            return;
        }
        
        // Transaction is valid - call Incentive API
        float incentiveAmount = 0f;
        try {
            Incentive incentive = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
            if (incentive != null) {
                incentiveAmount = incentive.getAmount();
                logger.info("Incentive amount received: {}", incentiveAmount);
            }
        } catch (Exception e) {
            logger.error("Error calling incentive API: {}", e.getMessage());
        }
        
        // Update balances: deduct from sender, add transaction amount + incentive to recipient
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
        
        userRecordRepository.save(sender);
        userRecordRepository.save(recipient);
        
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRecordRepository.save(record);
        
        logger.info("Transaction processed successfully: {} -> {}, amount={}, incentive={}", 
            sender.getId(), recipient.getId(), transaction.getAmount(), incentiveAmount);
    }
}
