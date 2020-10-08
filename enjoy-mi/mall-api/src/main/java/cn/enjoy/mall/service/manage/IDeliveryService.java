package cn.enjoy.mall.service.manage;

import cn.enjoy.mall.model.DeliveryDoc;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

/**
 * @author Ray
 * @date 2018/3/22.
 */
@RequestMapping("/order/mall/service/manage/IDeliveryService")
public interface IDeliveryService {

    @RequestMapping(value = "/queryDeliveryDocByOrderId", method = RequestMethod.POST)
    List<DeliveryDoc> queryDeliveryDocByOrderId(@RequestParam("orderId") Long orderId);

    @RequestMapping(value = "/shipping", method = RequestMethod.POST)
    void shipping(@RequestParam("deliveryDocStr") String deliveryDocStr, @RequestBody List<Long> orderGoodsIds);
}
