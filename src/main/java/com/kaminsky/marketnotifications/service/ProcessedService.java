package com.kaminsky.marketnotifications.service;

import com.kaminsky.entity.MarketOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class ProcessedService {
    private final Logger logger = LoggerFactory.getLogger(ProcessedService.class);

    @Async
    @RetryableTopic(backoff = @Backoff(delay = 3000))
    @KafkaListener(topics = "sent_orders", groupId = "sent_group")
    public void listen(MarketOrder order) {
        try {
            System.out.println("Ваш заказ под номером " + order.getId() + "имеет статус " + order.getStatus().name());
            logger.info("Заказ доставлен: {}", order);
        } catch (Exception e) {
            logger.error("Ошибка при обработке заказа: {}", order, e);
            throw e;
        }
    }
}
