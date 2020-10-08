package cn.enjoy.kill.service.mq;

import cn.enjoy.mall.service.IKillOrderService;
import cn.enjoy.mall.vo.KillOrderVo;
import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 类说明：
 */
@Slf4j
@Component
public class SecKillReceiver {

    @Resource
    private IKillOrderService orderService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @RabbitListener(queues = "order.seckill.producer"/*,errorHandler = "rabbitConsumerListenerErrorHandler"*/)
    @RabbitHandler // 此注解加上之后可以接受对象型消息
    public String process(Message message, Channel channel, @Headers Map<String, Object> headers) throws Exception {
        try {
            String msg = new String(message.getBody());
            log.info("UserReceiver>>>>>>>接收到消息:" + msg);
            try {
                KillOrderVo vo = JSON.parseObject(msg, KillOrderVo.class);
//                String kill_order_user = KillConstants.KILL_ORDER_USER + vo.getKillGoodsSpecPriceDetailVo().getId() + vo.getUserId();
//                if (null != stringRedisTemplate.opsForValue().get(kill_order_user)){//未超时，则业务处理
                Long orderId = orderService.killOrder(vo);
//                    String oldstr = stringRedisTemplate.opsForValue().getAndSet(kill_order_user,String.valueOf(orderId));
//                    if (null == oldstr){//已超时，生产端已拒绝
//                        orderService.cancel(orderId);
//                        stringRedisTemplate.delete(kill_order_user);
//                    }
//                }
                log.info("UserReceiver>>>>>>消息已消费");
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);//手工确认，可接下一条
                return orderId.toString();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);//失败，则直接忽略此订单
                log.info("UserReceiver>>>>>>拒绝消息，直接忽略");
                throw e;
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        return "";
    }
}

