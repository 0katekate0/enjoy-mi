package cn.enjoy.mall.service;

import cn.enjoy.mall.model.Order;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;
import java.util.Map;

@RequestMapping("/order/mall/service/IPayService")
public interface IPayService {

    @RequestMapping(value = "/doPrePay", method = RequestMethod.POST)
    Map<String, String> doPrePay(@RequestParam("orderId") Long orderId, @RequestParam("payCode") String payCode,
                                 @RequestParam("payAmount") BigDecimal payAmount, @RequestParam("userId") String userId) ;

    @RequestMapping(value = "/updateByActionId", method = RequestMethod.POST)
    String updateByActionId(@RequestParam("actionId") String actionId) ;

    @RequestMapping(value = "/updateStatusByActionId", method = RequestMethod.POST)
    Order updateStatusByActionId(@RequestParam("actionId") String actionId) ;

    @RequestMapping(value = "/queryByPrepayId", method = RequestMethod.POST)
    String queryByPrepayId(@RequestParam("prepayId") String prepayId) ;

    @RequestMapping(value = "/doPay", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    String doPay(@RequestParam("orderId") Long orderId, @RequestParam("payCode") String payCode,
                 @RequestParam("payAmount") BigDecimal payAmount, @RequestParam("userId") String userId) ;
}
