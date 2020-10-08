package cn.enjoy.mall.service;

import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderAction;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

@RequestMapping("/kill/order/service/IOrderActionService")
public interface IKillOrderActionService {
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    void save(@RequestBody Order order, @RequestParam("actinNote") String actinNote, @RequestParam("userId") String userId);

    @RequestMapping(value = "/savePre", method = RequestMethod.POST)
    Long savePre(@RequestParam("orderStr") String orderStr, @RequestBody Map map,
                @RequestParam("action") String action, @RequestParam("userId") String userId,
                @RequestParam("remark") String remark);

    @RequestMapping(value = "/savePresync", method = RequestMethod.POST)
    Long savePre(@RequestParam("orderStr") String orderStr,
                 @RequestParam("action") String action, @RequestParam("userId") String userId,
                 @RequestParam("remark") String remark);
    /**
     * 写订单日志
     * @param order
     * @param action 本次做的操作，对应status_desc
     * @param userId
     * @param remark 备注，对应action_note
     */
    @RequestMapping(value = "/save2", method = RequestMethod.POST)
    void save(@RequestBody Order order, @RequestParam("action") String action, @RequestParam("userId") String userId, @RequestParam("remark") String remark);

    @RequestMapping(value = "/queryByPrepayId", method = RequestMethod.POST)
    OrderAction queryByPrepayId(@RequestParam("prepayId") String prepayId);

    @RequestMapping(value = "/updatePre", method = RequestMethod.POST)
    Long updatePre(@RequestParam("actionId") Long actionId, @RequestBody Map map) ;

    @RequestMapping(value = "/queryByOrderId", method = RequestMethod.POST)
    List<OrderAction> queryByOrderId(@RequestParam("orderId") Long orderId);
}
