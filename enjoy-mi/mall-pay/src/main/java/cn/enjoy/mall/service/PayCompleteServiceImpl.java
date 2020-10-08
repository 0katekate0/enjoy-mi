package cn.enjoy.mall.service;

import cn.enjoy.mall.model.*;
import cn.enjoy.mall.service.manage.IKillSpecManageService;
import io.seata.spring.annotation.GlobalTransactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 支付完成后的业务处理
 *
 * @Classname PayCompleteServiceImpl
 * @Description TODO
 * @Author Jack
 * Date 2020/8/21 21:01
 * Version 1.0
 */
@Service
public class PayCompleteServiceImpl implements PayCompleteService {

    @Autowired
    private IKillOrderService killorderService;

    @Autowired
    private IOrderService orderService;

    @Autowired
    private IOrderActionService orderActionService;

    @Autowired
    private IKillOrderActionService killOrderActionService;

    @Autowired
    private IKillSpecManageService killSpecManageService;

    @Autowired
    private IGoodsService goodsService;

    @Autowired
    private IPayService iPayService;

    @Autowired
    private IKillPayService iKillPayService;

    @GlobalTransactional
    @Override
    public void payCompleteBusiness(String actionId) {
        //1、根据订单id查询是什么订单
//        Order order = orderService.selectOrderDetail(Long.valueOf(orderId));
        OrderAction orderAction = orderActionService.queryByActionId(Long.valueOf(actionId));
        if (orderAction != null) {
//            String orderStr = JSONObject.toJSONString(order);
//            orderActionService.savePre(orderStr, "微信支付成功", order.getUserId(), "微信支付成功");
//            order.setPayStatus(PayStatus.PAID.getCode());
//            order.setPayCode("weixin");
//            order.setPayName(PayType.getDescByCode("weixin"));
//            order.setPayTime(System.currentTimeMillis());
//            orderService.updateOrder(order);
            Order order = iPayService.updateStatusByActionId(actionId);

            List<SpecGoodsPrice> sgps = new ArrayList<>();
            for (OrderGoods orderGoods : order.getOrderGoodsList()) {
                SpecGoodsPrice specGoodsPrice = new SpecGoodsPrice();
                specGoodsPrice.setId(orderGoods.getSpecGoodsId());
                specGoodsPrice.setStoreCount(Integer.valueOf(orderGoods.getGoodsNum()));
                sgps.add(specGoodsPrice);
            }
            //扣减库存
            goodsService.updateBySpecGoodsIds(sgps);
        } else {
//            Order killorder = killorderService.search(Long.valueOf(orderId));
//            String orderStr = JSONObject.toJSONString(killorder);
//            killOrderActionService.savePre(orderStr, "微信支付成功", killorder.getUserId(), "微信支付成功");
//            killorder.setPayStatus(PayStatus.PAID.getCode());
//            killorder.setPayCode("weixin");
//            killorder.setPayName(PayType.getDescByCode("weixin"));
//            killorder.setPayTime(System.currentTimeMillis());
//            killorderService.updateOrder(killorder);
            Order order = iKillPayService.updateStatusByActionId(actionId);

            KillGoodsPrice killGoodsPrice = new KillGoodsPrice();
            killGoodsPrice.setSpecGoodsId(order.getOrderGoodsList().get(0).getSpecGoodsId());
            killGoodsPrice.setKillCount(1);
            //扣减库存
            killSpecManageService.updateBySpecGoodsId(killGoodsPrice);
        }
    }
}
