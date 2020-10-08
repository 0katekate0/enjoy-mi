package cn.enjoy.test;

import cn.enjoy.OrderServiceApp;
import cn.enjoy.mall.dao.ScheduledMapper;
import cn.enjoy.mall.model.HotSellingGoods;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = OrderServiceApp.class)
public class MyTest {

    @Autowired
    private ScheduledMapper scheduledMapper;

    @Test
    public void test1() {
        List<HotSellingGoods> hotSellingGoods = scheduledMapper.hotProduct(5);
        System.out.println(JSONObject.toJSON(hotSellingGoods));
    }
}
