package cn.enjoy.mall.service;


import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping("/mall/service/IScheduledService")
public interface IScheduledService {
    @RequestMapping(value = "/scheduledHotProduct")
    void scheduledHotProduct(); //定时任务统计热门商品
}
