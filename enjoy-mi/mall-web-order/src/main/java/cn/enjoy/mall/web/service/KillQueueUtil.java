package cn.enjoy.mall.web.service;

import cn.enjoy.mall.constant.KillConstants;
import cn.enjoy.mall.lock.RedisLock;
import cn.enjoy.mall.model.KillGoodsPrice;
import cn.enjoy.mall.service.manage.IKillSpecManageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.Timer;
import java.util.concurrent.*;

/**
 * @Classname KillQueueUtil
 * @Description TODO
 * @Author Jack
 * Date 2020/9/28 20:15
 * Version 1.0
 */
@Component
@Slf4j
public class KillQueueUtil {

    private Queue<KUBean> queue = new ConcurrentLinkedQueue<>();

    // 线程池
    private ExecutorService executorService = Executors.newCachedThreadPool();

    // 4个线程调度
    private ScheduledExecutorService ses = Executors.newScheduledThreadPool(4);

    @Autowired
    private KillGoodsService killGoodsService;
    @Autowired
    private RedisTemplate redisTemplate;
    @Autowired
    private IKillSpecManageService iKillSpecManageService;

    public void addQueue(KUBean kuBean) {
        queue.offer(kuBean);
    }

    public KillQueueUtil() {
        execute();
    }

    private void execute() {
        // 本地线程消费消息
        ses.scheduleWithFixedDelay(() -> {
            // 获取队列消息
            KUBean kubean = queue.poll();
            if(kubean != null) {
                stock(kubean.getKillId(), kubean.getUserId());
            }
        }, 0, 1, TimeUnit.MILLISECONDS);
    }

    private void stock(Integer killId,String userId) {
        String killGoodCount = KillConstants.KILL_GOOD_COUNT + killId;
        // 返回的数值,执行了lua脚本
        Long stock = killGoodsService.stock(killGoodCount, 1, killGoodsService.STOCK_LUA);
        if (stock == killGoodsService.UNINITIALIZED_STOCK) {
            Timer timer = null;
            RedisLock redisLock = new RedisLock(redisTemplate, "stock:lock");
            try {
                // 如果竞争锁成功  如果其他线程没竞争锁成功，这里是阻塞的
                if (redisLock.tryLock()) {
                    stock = killGoodsService.stock(killGoodCount, 1, killGoodsService.STOCK_LUA);
                    if (stock == killGoodsService.UNINITIALIZED_STOCK) {
                        KillGoodsPrice killGoodsPrice = iKillSpecManageService.selectByPrimaryKey(killId);
                        redisTemplate.opsForValue().set(killGoodCount, killGoodsPrice.getKillCount(), 60 * 60, TimeUnit.SECONDS);
                        // 再次去执行lua脚本，扣减库存
                        stock = killGoodsService.stock(killGoodCount, 1, killGoodsService.STOCK_LUA);
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (timer != null) {
                    timer.cancel();
                }

                //释放锁 。自己加的锁不能让别人释放，自己只能释放自己的锁
                //这里要进行一个value值的比较，只要自己的value值相等才能释放
                redisLock.unlock();
            }
        }
    }
}
