package cn.enjoy.mall.service.impl;

import cn.enjoy.mall.constant.PayStatus;
import cn.enjoy.mall.constant.PayType;
import cn.enjoy.mall.dao.OrderActionMapper;
import cn.enjoy.mall.dao.OrderMapper;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderAction;
import cn.enjoy.mall.service.IKillPayService;
import cn.enjoy.mall.service.IOrderActionService;
import cn.enjoy.mall.service.IPayService;
import cn.enjoy.mall.service.IWxPayService;
import com.alibaba.fastjson.JSONObject;
import com.baidu.fsg.uid.impl.DefaultUidGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 支付管理
* @author Jack
* @date 2020/9/8
*/
@Slf4j
@RestController
//@RequestMapping("/order/mall/service/IPayService")
public class PayServiceImpl implements IPayService {
    @Resource
    private OrderMapper orderMapper;
    @Resource
    private OrderActionMapper orderActionMapper;
    @Resource
    private IOrderActionService orderActionService;
    @Autowired
    private IWxPayService iWxPayService;

    @Autowired
    private IKillPayService killPayService;

    @Autowired
    private DefaultUidGenerator defaultUidGenerator;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 微信预支付
     *
     * @param orderId
     * @param payCode
     * @param payAmount
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/doPrePay", method = RequestMethod.POST)
    @Transactional
    @Override
    public Map<String, String> doPrePay(Long orderId, String payCode, BigDecimal payAmount, String userId) {

        Order order = orderMapper.selectByPrimaryKey(orderId);
        //如果查询不到订单，则是秒杀订单
        if (order == null) {
            log.info("-----------killPayService.doPrePay--------");
            return killPayService.doPrePay(orderId, payCode, payAmount, userId);
        }
        Map<String, String> return_map = new HashMap<>();
        if (payAmount.compareTo(order.getOrderAmount()) != 0) {
            return_map.put("result_code", "fail");
            return_map.put("return_msg", "支付金额不正确");
            return return_map;
        }
        String payName = PayType.getDescByCode(payCode);
        if (StringUtils.isEmpty(payName)) {
            return_map.put("result_code", "fail");
            return_map.put("return_msg", "支付方式不存在");
            return return_map;
        }
        if (order.getPayStatus() == 1) {
            return_map.put("result_code", "fail");
            return_map.put("return_msg", "此订单已经付款完成！");
            return return_map;
        }
        order.setPayStatus(PayStatus.UNPAID.getCode());
        order.setPayCode(payCode);
        order.setPayName(PayType.getDescByCode(payCode));
        order.setPayTime(System.currentTimeMillis());
        orderMapper.updateByPrimaryKeySelective(order);
        String orderStr = JSONObject.toJSONString(order);
        Long action_id = orderActionService.savePre(orderStr, null, "微信-预支付订单", userId, "微信-预支付订单");
        Map<String, String> map = iWxPayService.unifiedorder(String.valueOf(action_id), payAmount, userId);
        orderActionService.updatePre(action_id, map);
        return map;
    }

    /**
     * 根据主键修改订单日志
     *
     * @param actionId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateByActionId", method = RequestMethod.POST)
    @Transactional
    @Override
    public String updateByActionId(String actionId) {
        OrderAction orderAction = orderActionMapper.selectByPrimaryKey(Long.parseLong(actionId));
        Order order = orderMapper.selectByPrimaryKey(orderAction.getOrderId());
        order.setOrderId(order.getOrderId());
        order.setPayStatus(PayStatus.PAID.getCode());
        order.setPayTime(System.currentTimeMillis());
        orderMapper.updateByPrimaryKeySelective(order);

        orderAction.setPayStatus(1);
        orderAction.setLogTime(System.currentTimeMillis());
        orderActionMapper.updateByPrimaryKey(orderAction);
        return "SUCCESS";
    }

    /**
     * seata修改订单和订单日志
     *
     * @param actionId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateStatusByActionId", method = RequestMethod.POST)
    @Override
    public Order updateStatusByActionId(String actionId) {
        OrderAction orderAction = orderActionMapper.selectByPrimaryKey(Long.parseLong(actionId));
        Order order = orderMapper.selectByPrimaryKey(orderAction.getOrderId());

        String updateOrderSql = "update tp_order set pay_status = ?,pay_time = ? where order_id = ?";
        jdbcTemplate.update(updateOrderSql, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setInt(1, PayStatus.PAID.getCode());
                ps.setLong(2, System.currentTimeMillis());
                ps.setLong(3, order.getOrderId());
            }
        });
        String orderActionSql = "update tp_order_action set pay_status = ?,log_time = ? where action_id = ?";
        jdbcTemplate.update(orderActionSql, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setInt(1, PayStatus.PAID.getCode());
                ps.setLong(2, System.currentTimeMillis());
                ps.setLong(3, orderAction.getActionId());
            }
        });
        return order;
    }

    /**
     * 支付校验
     *
     * @param orderId
     * @param payCode
     * @param payAmount
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/doPay", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
//    @Transactional
    @Override
    public String doPay(Long orderId, String payCode, BigDecimal payAmount, String userId) {
//        Order order = orderMapper.selectByPrimaryKey(orderId);
//        if(payAmount.compareTo(order.getOrderAmount())!=0){
//            return "支付金额不正确";
//        }
//        String payName = PayType.getDescByCode(payCode);
//        if(StringUtils.isEmpty(payName)){
//            return "支付方式不存在";
//        }
//        order.setPayStatus(PayStatus.PAID.getCode());
//        order.setPayCode(payCode);
//        order.setPayName(PayType.getDescByCode(payCode));
//        order.setPayTime(System.currentTimeMillis());
//        orderMapper.updateByPrimaryKeySelective(order);
//        orderActionService.save(order,"支付成功",userId);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("respCode", "0");
        jsonObject.put("respMsg", "成功");
        return jsonObject.toJSONString();
    }

    /**
     * 校验是否支付成功
     *
     * @param prepayId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPrepayId", method = RequestMethod.POST)
    @Override
    public String queryByPrepayId(String prepayId) {
        OrderAction orderAction = orderActionService.queryByPrepayId(prepayId);
        if (orderAction == null) {
            return killPayService.queryByPrepayId(prepayId);
        } else {
            if (orderAction.getPayStatus() != null) {
                return orderAction.getPayStatus().toString();
            } else {
                return "0";
            }
        }
    }
}
