package com.kaminsky.marketnotifications.service;

import com.kaminsky.marketnotifications.entity.ProcessedOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class ProcessedService {
    private final JavaMailSender mailSender;

    private final Logger logger = LoggerFactory.getLogger(ProcessedService.class);

    private String emailAddress = "kaminsky8836@gmail.com";

    @Autowired
    public ProcessedService(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }

    @RetryableTopic(backoff = @Backoff(delay = 3000))
    @KafkaListener(topics = "sent_orders", groupId = "sent_group")
    public void listen(ProcessedOrder order) {
        try {
            logger.info("Новый заказ: {}", order);

            SimpleMailMessage email = new SimpleMailMessage();
            email.setTo(emailAddress);
            email.setSubject("Заказ " + order.getId());
            email.setText("Ваш заказ под номером " + order.getId() + "имеет статус " + order.getStatus());
            mailSender.send(email);
        } catch (Exception e) {
            logger.error("Ошибка при обработке заказа: {}", order, e);
            throw e;
        }
    }
}
