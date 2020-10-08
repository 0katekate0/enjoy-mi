package cn.enjoy.mall.web;

import cn.enjoy.FrontApp;
import cn.enjoy.mall.web.service.KillGoodsService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.redisson.RedissonRedLock;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Classname MyTest
 * @Description TODO
 * @Author Jack
 * Date 2020/9/17 13:58
 * Version 1.0
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {FrontApp.class})
public class MyTest {

    private Logger logger = LoggerFactory.getLogger(getClass());


    private static Integer count = 100;

    private CountDownLatch cdl = new CountDownLatch(count);

    @Autowired
    private KillGoodsService killGoodsService;

    @Test
    public void test1() {
        for (Integer i = 0; i < count; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        cdl.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    try {
                        killGoodsService.redissonIncr();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//                    logger.info("===========>" + killGoodsService.getI());
                }
            }).start();
            cdl.countDown();
        }

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test2() {
        killGoodsService.secKillByRedissonLock(24,"b9702959ebc44774912a59a012d6abfe");
    }

    @Autowired
    private RedisTemplate redisTemplate;
    @Test
    public void test3() {
        redisTemplate.opsForValue().set("fdasfd",90);
    }

    @Autowired
    @Qualifier("redissonClient1")
    private RedissonClient redissonClient1;

    @Autowired
    @Qualifier("redissonClient2")
    private RedissonClient redissonClient2;

    @Autowired
    @Qualifier("redissonClient3")
    private RedissonClient redissonClient3;

    int j = 0;

    // 解决主从锁失效的问题
    @Test
    public void redLockTest() {

        String resourceName = "test_redlock";
        // 三台主机竞争同一把锁，超过半数竞争成功才会显示锁竞争成功
        RLock lock1 = redissonClient1.getLock(resourceName);
        RLock lock2 = redissonClient2.getLock(resourceName);
        RLock lock3 = redissonClient3.getLock(resourceName);
        // 三把锁获取到redLock
        RedissonRedLock redLock = new RedissonRedLock(lock1, lock2, lock3);
//        try {
//            redLock.lock(5, TimeUnit.SECONDS);
//            logger.info("===========>" + ++j);
//        } finally {
//            redLock.unlock();
//        }

        for (Integer i = 0; i < count; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        cdl.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    try {
                        // 阻塞
                        redLock.lock(5, TimeUnit.SECONDS);

                        // 业务执行...
                        logger.info("===========>" + ++j);
                    } finally {
                        redLock.unlock();
                    }
                }
            }).start();
            cdl.countDown();
        }

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
