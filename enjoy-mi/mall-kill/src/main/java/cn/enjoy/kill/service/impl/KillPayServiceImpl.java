package cn.enjoy.kill.service.impl;

import cn.enjoy.kill.dao.OrderActionMapper;
import cn.enjoy.kill.dao.OrderMapper;
import cn.enjoy.mall.constant.PayStatus;
import cn.enjoy.mall.constant.PayType;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderAction;
import cn.enjoy.mall.service.IKillOrderActionService;
import cn.enjoy.mall.service.IKillPayService;
import cn.enjoy.mall.service.IWxPayService;
import com.alibaba.fastjson.JSONObject;
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
 * 秒杀商品支付管理
 *
 * @author Jack
 * @date 2020/9/8
 */
@RestController
//@RequestMapping("/kill/order/service/IPayService")
public class KillPayServiceImpl implements IKillPayService {

    @Resource
    private OrderMapper orderMapper;
    @Resource
    private OrderActionMapper orderActionMapper;
    @Resource
    private IKillOrderActionService orderActionService;
    @Autowired
    private IWxPayService iWxPayService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 秒杀商品预支付
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
    //@RequestMapping(value = "/doPrePay", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Transactional
    @Override
    public Map<String, String> doPrePay(Long orderId, String payCode, BigDecimal payAmount, String userId) {
        Order order = orderMapper.selectByPrimaryKey(orderId);
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
     * 修改订单日志
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
     * 修改订单和订单日志
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

        String updateOrderSql = "update tp_order_kill set pay_status = ?,pay_time = ? where order_id = ?";
        jdbcTemplate.update(updateOrderSql, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setInt(1, PayStatus.PAID.getCode());
                ps.setLong(2, System.currentTimeMillis());
                ps.setLong(3, order.getOrderId());
            }
        });
        String orderActionSql = "update tp_order_action_kill set pay_status = ?,log_time = ? where action_id = ?";
        jdbcTemplate.update(orderActionSql, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setInt(1, PayStatus.PAID.getCode());
                ps.setLong(2, System.currentTimeMillis());
                ps.setLong(3, orderAction.getActionId());
            }
        });
        if (false) throw new RuntimeException("异常测试");
        return order;
    }

    /**
     * 支付成功后修改状态
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
    @Transactional
    @Override
    public String doPay(Long orderId, String payCode, BigDecimal payAmount, String userId) {
        Order order = orderMapper.selectByPrimaryKey(orderId);
        if (payAmount.compareTo(order.getOrderAmount()) != 0) {
            return "支付金额不正确";
        }
        String payName = PayType.getDescByCode(payCode);
        if (StringUtils.isEmpty(payName)) {
            return "支付方式不存在";
        }
        order.setPayStatus(PayStatus.PAID.getCode());
        order.setPayCode(payCode);
        order.setPayName(PayType.getDescByCode(payCode));
        order.setPayTime(System.currentTimeMillis());
        orderMapper.updateByPrimaryKeySelective(order);
        orderActionService.save(order, "支付成功", userId);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("respCode", "0");
        jsonObject.put("respMsg", "成功");
        return jsonObject.toJSONString();
    }

    /**
     * 判断支付是否成功
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
        if (orderAction != null && orderAction.getPayStatus() != null) {
            return orderAction.getPayStatus().toString();
        } else {
            return "0";
        }
    }
}
