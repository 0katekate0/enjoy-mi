package cn.enjoy.kill.service.impl;

import cn.enjoy.kill.dao.OrderActionMapper;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderAction;
import cn.enjoy.mall.service.IKillOrderActionService;
import com.alibaba.fastjson.JSONObject;
import com.baidu.fsg.uid.impl.CachedUidGenerator;
import com.baidu.fsg.uid.impl.DefaultUidGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 订单日志
 * @author Jack
 */
@RestController
//@RequestMapping("/kill/order/service/IOrderActionService")
public class OrderActionServiceImpl implements IKillOrderActionService {
    @Resource
    private OrderActionMapper orderActionMapper;

    @Autowired
    private DefaultUidGenerator defaultUidGenerator;

    @Autowired
    private CachedUidGenerator cachedUidGenerator;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 保存订单日志
    * @param order
     * @param action
     * @param userId
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/save", method = RequestMethod.POST)
    @Override
    public void save(Order order, String action, String userId) {
        this.save(order, action, userId, null);
    }
    /**
     * 保存预支付订单日志
    * @param orderStr
     * @param map
     * @param action
     * @param userId
     * @param remark
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/savePre", method = RequestMethod.POST)
    @Override
    public Long savePre(String orderStr, Map map , String action, String userId, String remark) {
        Order order = JSONObject.parseObject(orderStr,Order.class);
        OrderAction orderAction = new OrderAction();
        orderAction.setActionId(defaultUidGenerator.getUID());
        orderAction.setActionUser(userId);
        orderAction.setLogTime(System.currentTimeMillis());
        orderAction.setOrderId(order.getOrderId());
        orderAction.setOrderStatus(order.getOrderStatus());
        orderAction.setPayStatus(order.getPayStatus());
        orderAction.setShippingStatus(order.getShippingStatus());
        orderAction.setStatusDesc(action);
        orderAction.setActionNote(remark);
        if(map !=null&&map.get("trade_type")!=null){
            orderAction.setTradeType(map.get("trade_type").toString());
        }
        if(map !=null&&map.get("prepay_id")!=null){
            orderAction.setPrepayId(map.get("prepay_id").toString());
        }
        if(map !=null&&map.get("code_url")!=null){
            orderAction.setCodeUrl(map.get("code_url").toString());
        }
        orderActionMapper.insert(orderAction);
        return orderAction.getActionId();
    }

    /**
     * seata保存预支付订单日志
     * @param orderStr
     * @param action
     * @param userId
     * @param remark
     * @author Jack
     * @date 2020/9/8
     * @throws Exception
     * @return
     * @version
     */
    //@RequestMapping(value = "/savePresync", method = RequestMethod.POST)
    @Override
    public Long savePre(String orderStr, String action, String userId, String remark) {
        Order order = JSONObject.parseObject(orderStr,Order.class);
        OrderAction orderAction = new OrderAction();
        orderAction.setActionId(defaultUidGenerator.getUID());
        orderAction.setActionUser(userId);
        orderAction.setLogTime(System.currentTimeMillis());
        orderAction.setOrderId(order.getOrderId());
        orderAction.setOrderStatus(order.getOrderStatus());
        orderAction.setPayStatus(order.getPayStatus());
        orderAction.setShippingStatus(order.getShippingStatus());
        orderAction.setStatusDesc(action);
        orderAction.setActionNote(remark);
        //mycat 获取DataMate会报错，所以用jdbcTemplate直接连接数据库，这样整合seata就没有问题
        String sql = "insert into tp_order_action_kill (action_id, order_id, action_user, trade_type, prepay_id, code_url,order_status, shipping_status, pay_status, action_note, log_time, status_desc) values (?,?,?,?,?,?,?,?,?,?,?,?)";
        jdbcTemplate.update(sql, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setLong(1,orderAction.getActionId());
                ps.setLong(2,orderAction.getOrderId());
                ps.setString(3,orderAction.getActionUser());
                ps.setString(4,orderAction.getTradeType());
                ps.setString(5,orderAction.getPrepayId());
                ps.setString(6,orderAction.getCodeUrl());
                ps.setInt(7,orderAction.getOrderStatus());
                ps.setInt(8,orderAction.getShippingStatus());
                ps.setInt(9,orderAction.getPayStatus());
                ps.setString(10,orderAction.getActionNote());
                ps.setLong(11,orderAction.getLogTime());
                ps.setString(12,orderAction.getStatusDesc());
            }
        });
//        orderActionMapper.insert(orderAction);
        return orderAction.getActionId();
    }

    /**
     * 修改预支付订单日志
    * @param actionId
     * @param map
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/updatePre", method = RequestMethod.POST)
    @Override
    public Long updatePre(Long actionId,Map map ) {
        OrderAction orderAction = orderActionMapper.selectByPrimaryKey(actionId);
        if(map.get("trade_type")!=null){
            orderAction.setTradeType(map.get("trade_type").toString());
        }
        if(map.get("prepay_id")!=null){
            orderAction.setPrepayId(map.get("prepay_id").toString());
        }
        if(map.get("code_url")!=null){
            orderAction.setCodeUrl(map.get("code_url").toString());
        }
        orderActionMapper.updateByPrimaryKey(orderAction);
        return orderAction.getActionId();
    }

    /**
     * 保存订单日志
    * @param order
     * @param action
     * @param userId
     * @param remark
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/save2", method = RequestMethod.POST)
    @Override
    public void save(Order order, String action, String userId, String remark) {
        OrderAction orderAction = new OrderAction();
        orderAction.setActionId(defaultUidGenerator.getUID());
        orderAction.setOrderType(order.getOrderType());
        orderAction.setActionUser(userId);
        orderAction.setLogTime(System.currentTimeMillis());
        orderAction.setOrderId(order.getOrderId());
        orderAction.setOrderStatus(order.getOrderStatus());
        orderAction.setPayStatus(order.getPayStatus());
        orderAction.setShippingStatus(order.getShippingStatus());
        orderAction.setStatusDesc(action);
        orderAction.setActionNote(remark);
        orderActionMapper.insert(orderAction);
    }

    /**
     * 根据prepayId查询订单日志
    * @param prepayId
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/queryByPrepayId", method = RequestMethod.POST)
    @Override
    public OrderAction queryByPrepayId(String prepayId) {
        OrderAction orderAction = new OrderAction();
        orderAction = orderActionMapper.queryByPrepayId(prepayId);
        return orderAction;
    }

    /**
     * 查询订单日志
    * @param orderId
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/queryByOrderId", method = RequestMethod.POST)
    @Override
    public List<OrderAction> queryByOrderId(Long orderId) {
        return orderActionMapper.queryByOrderId(orderId);
    }
}
