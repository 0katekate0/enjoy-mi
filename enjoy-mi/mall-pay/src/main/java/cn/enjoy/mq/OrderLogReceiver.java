package cn.enjoy.mq;

import cn.enjoy.mall.service.PayCompleteService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 类说明：
 */
@Slf4j
@Component
public class OrderLogReceiver implements ChannelAwareMessageListener {

    @Autowired
    private PayCompleteService payCompleteService;

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        try {
            String orderId = new String(message.getBody());
            log.info("OrderLogReceiver>>>>>>>接收到消息:" + orderId);
            try {
                payCompleteService.payCompleteBusiness(orderId);
                log.info("OrderLogReceiver>>>>>>消息已消费");
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);//手工确认，可接下一条
            } catch (Exception e) {
                System.out.println(e.getMessage());
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);//失败，则直接忽略此订单
                log.info("OrderLogReceiver>>>>>>拒绝消息，直接忽略");
                throw e;
            }

        } catch (Exception e) {
            log.info(e.getMessage());
        }

    }
}

