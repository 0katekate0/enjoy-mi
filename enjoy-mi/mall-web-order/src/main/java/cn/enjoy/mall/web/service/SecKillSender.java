package cn.enjoy.mall.web.service;

import cn.enjoy.mall.config.RabbitConfig;
import cn.enjoy.mall.vo.KillOrderVo;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * 类说明：
 */
@Slf4j
@Component
public class SecKillSender {
    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void send(KillOrderVo vo) {
        String msg1 = JSON.toJSONString(vo);
        log.info("TopicSender send the 1st : " + msg1);
        this.rabbitTemplate.convertAndSend(RabbitConfig.EXCHANGE_SECKILL, RabbitConfig.KEY_SECKILL, msg1);
    }

    public String sendAndReceive(KillOrderVo vo) {
        String msg1 = JSON.toJSONString(vo);
        //设置消息唯一id
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        //直接发送message对象
        MessageProperties messageProperties = new MessageProperties();
        //过期时间10秒,也是为了减少消息挤压的可能
//        messageProperties.setExpiration("10000");
        messageProperties.setCorrelationId(correlationId.getId());
        Message message = new Message(msg1.getBytes(), messageProperties);
        log.info("TopicSender send the 1st : " + msg1);
        //设置消息唯一id
        Message message1 = rabbitTemplate.sendAndReceive(RabbitConfig.EXCHANGE_SECKILL, RabbitConfig.KEY_SECKILL, message, correlationId);
        return new String(message1.getBody());
    }
}
