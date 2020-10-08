package cn.enjoy.mq;

import cn.enjoy.config.RabbitConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 类说明：
 */
@Slf4j
@Component
public class OrderLogSender {
    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void send(String orderId) {
        log.info("TopicSender send the 1st : " + orderId);
        this.rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE_LOG, RabbitConfig.KEY_LOG, orderId);
    }
}
