package cn.enjoy.kill.service.impl;

import cn.enjoy.core.exception.BusinessException;
import cn.enjoy.kill.dao.OrderGoodsMapper;
import cn.enjoy.kill.dao.OrderMapper;
import cn.enjoy.kill.dao.UserAddressMapper;
import cn.enjoy.mall.constant.MallConstant;
import cn.enjoy.mall.constant.OrderStatus;
import cn.enjoy.mall.constant.PayStatus;
import cn.enjoy.mall.constant.ShippingStatus;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderGoods;
import cn.enjoy.mall.model.SpecGoodsPrice;
import cn.enjoy.mall.model.UserAddress;
import cn.enjoy.mall.mongo.GoodsDao;
import cn.enjoy.mall.service.IKillOrderActionService;
import cn.enjoy.mall.service.IKillOrderService;
import cn.enjoy.mall.vo.GoodsVo;
import cn.enjoy.mall.vo.KillGoodsSpecPriceDetailVo;
import cn.enjoy.mall.vo.KillOrderVo;
import com.baidu.fsg.uid.impl.CachedUidGenerator;
import com.baidu.fsg.uid.impl.DefaultUidGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 秒杀订单管理
 */
@Slf4j
@RestController
//@RequestMapping("/kill/order/service/IKillOrderService")
public class OrderServiceImpl implements IKillOrderService {
    @Resource
    private OrderMapper orderMapper;
    @Resource
    private OrderGoodsMapper orderGoodsMapper;
    @Resource
    private UserAddressMapper userAddressMapper;
    @Resource
    private GoodsDao goodsDao;
    @Resource
    private IKillOrderActionService orderActionService;
    @Resource
    private SequenceGenerator sequenceGenerator;

    @Autowired
    private DefaultUidGenerator defaultUidGenerator;

    @Autowired
    private CachedUidGenerator cachedUidGenerator;

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 订单详情
     *
     * @param orderId
     * @return
     */
    //@GetMapping("detail/{orderId}")
    public Order search(@PathVariable("orderId") Long orderId) {
        Order order = orderMapper.selectByPrimaryKey(orderId);
        log.info("----order:-----" + order);
        if (order != null) {
            List<OrderGoods> goodsList = orderGoodsMapper.selectByOrderId(orderId);
            order.setOrderGoodsList(goodsList);
        }
        return order;
    }

    /**
     * 查询用户的订单
     *
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryOrderByUserId", method = RequestMethod.POST)
    @Override
    public List<Order> queryOrderByUserId(String userId) {
        Map map = new HashMap();
        map.put("userId", userId);
        return orderMapper.queryByPage(map);
    }

    @Override
    public Integer queryCountByUserId(String userId) {
        return orderMapper.selectOrderNum(1,userId);
    }

    /**
     * 秒杀商品保存订单
     *
     * @param addressId
     * @param killGoods
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/killOrder", method = RequestMethod.POST)
    @Transactional
    public Long killOrder(Long addressId, KillGoodsSpecPriceDetailVo killGoods, String userId) {
        //创建一个订单
        Order order = new Order();
//        order.setOrderType("K");
        //从SequenceGenerator中获取订单的变化
        order.setOrderId(defaultUidGenerator.getUID());
        order.setOrderSn(sequenceGenerator.getOrderNo());
        order.setAddTime(System.currentTimeMillis());
        //设置订单的状态为未确定订单
        order.setOrderStatus(OrderStatus.UNCONFIRMED.getCode());
        //未支付
        order.setPayStatus(PayStatus.UNPAID.getCode());
        //未发货
        order.setShippingStatus(ShippingStatus.UNSHIPPED.getCode());
        //获取发货地址
        UserAddress userAddress = userAddressMapper.selectByPrimaryKey(addressId);
        BeanUtils.copyProperties(userAddress, order);
        order.setUserId(userId);
//        redisTemplate.opsForValue().set(order.getOrderId(),order);
        //新增订单
        orderMapper.insert(order);
        Long orderId = order.getOrderId();
        BigDecimal totalAmount = new BigDecimal(0);
        //从mongodb的购物车中获取所购物品
        List<OrderGoods> orderGoodsList = new ArrayList<>();
        GoodsVo goodsVo = goodsDao.findOneBySpecGoodsId(killGoods.getSpecGoodsId());

        //创建的订单商品
        OrderGoods orderGoods = new OrderGoods();
        orderGoods.setRecId(defaultUidGenerator.getUID());
        orderGoods.setOrderType("K");
        orderGoods.setOrderId(orderId);
        BeanUtils.copyProperties(goodsVo.getBase(), orderGoods);
        orderGoods.setPromType(true);
        orderGoods.setPromId(killGoods.getId());
        orderGoods.setGoodsNum((short) 1);
        orderGoods.setGoodsPrice(killGoods.getPrice());
        orderGoods.setSpecKey(killGoods.getKey());
        orderGoods.setSpecKeyName(killGoods.getKeyName());
        orderGoods.setSpecGoodsId(killGoods.getSpecGoodsId());
        orderGoods.setOriginalImg(killGoods.getOriginalImg());
        orderGoodsList.add(orderGoods);
        totalAmount = totalAmount.add(killGoods.getPrice());
        order.setGoodsPrice(totalAmount);
        order.setShippingPrice(new BigDecimal(0));
        order.setOrderAmount(totalAmount.add(order.getShippingPrice()));
        order.setTotalAmount(totalAmount.add(order.getShippingPrice()));

        //修改订单
        orderMapper.updateByPrimaryKeySelective(order);

        //保存订单产品信息
        orderGoodsMapper.insertBatch(orderGoodsList);
        //订单日志
        orderActionService.save(order, "创建秒杀订单", userId);

        if (redisTemplate.hasKey(userId)) {
            //清空用于分页的缓存
            redisTemplate.delete(userId);
        }
        return orderId;
    }

    @Transactional
    public Long killOrder(Long addressId, KillGoodsSpecPriceDetailVo killGoods, String userId,Long orderId) {
        //创建一个订单
        Order order = new Order();
//        order.setOrderType("K");
        //从SequenceGenerator中获取订单的变化
        order.setOrderId(orderId);
        order.setOrderSn(sequenceGenerator.getOrderNo());
        order.setAddTime(System.currentTimeMillis());
        //设置订单的状态为未确定订单
        order.setOrderStatus(OrderStatus.UNCONFIRMED.getCode());
        //未支付
        order.setPayStatus(PayStatus.UNPAID.getCode());
        //未发货
        order.setShippingStatus(ShippingStatus.UNSHIPPED.getCode());
        //获取发货地址
        UserAddress userAddress = userAddressMapper.selectByPrimaryKey(addressId);
        BeanUtils.copyProperties(userAddress, order);
        order.setUserId(userId);

        redisTemplate.opsForValue().set(orderId,order);
        //新增订单
        orderMapper.insert(order);
        BigDecimal totalAmount = new BigDecimal(0);
        //从mongodb的购物车中获取所购物品
        List<OrderGoods> orderGoodsList = new ArrayList<>();
        GoodsVo goodsVo = goodsDao.findOneBySpecGoodsId(killGoods.getSpecGoodsId());

        //创建的订单商品
        OrderGoods orderGoods = new OrderGoods();
        orderGoods.setRecId(defaultUidGenerator.getUID());
        orderGoods.setOrderType("K");
        orderGoods.setOrderId(orderId);
        BeanUtils.copyProperties(goodsVo.getBase(), orderGoods);
        orderGoods.setPromType(true);
        orderGoods.setPromId(killGoods.getId());
        orderGoods.setGoodsNum((short) 1);
        orderGoods.setGoodsPrice(killGoods.getPrice());
        orderGoods.setSpecKey(killGoods.getKey());
        orderGoods.setSpecKeyName(killGoods.getKeyName());
        orderGoods.setSpecGoodsId(killGoods.getSpecGoodsId());
        orderGoods.setOriginalImg(killGoods.getOriginalImg());
        orderGoodsList.add(orderGoods);
        totalAmount = totalAmount.add(killGoods.getPrice());
        order.setGoodsPrice(totalAmount);
        order.setShippingPrice(new BigDecimal(0));
        order.setOrderAmount(totalAmount.add(order.getShippingPrice()));
        order.setTotalAmount(totalAmount.add(order.getShippingPrice()));

        //修改订单
        orderMapper.updateByPrimaryKeySelective(order);

        //保存订单产品信息
        orderGoodsMapper.insertBatch(orderGoodsList);
        //订单日志
        orderActionService.save(order, "创建秒杀订单", userId);

        if (redisTemplate.hasKey(userId)) {
            //清空用于分页的缓存
            redisTemplate.delete(userId);
        }
        return orderId;
    }

    /**
     * 保存订单
     *
     * @param killOrderVo
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/killOrder2", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Transactional
    @Override
    public Long killOrder(KillOrderVo killOrderVo) {
        return this.killOrder(killOrderVo.getAddressId(),
                killOrderVo.getKillGoodsSpecPriceDetailVo(), killOrderVo.getUserId());
    }

    /**
     * 查询订单详情
     *
     * @param orderId
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectMyOrderDetail", method = RequestMethod.POST)
    @Override
    public Order selectMyOrderDetail(Long orderId, String userId) {
        Order order = orderMapper.selectByPrimaryKey(orderId);
        if (order != null) {
            if (StringUtils.isEmpty(userId) || !userId.equals(order.getUserId())) {
                return null;
            }
            List<OrderGoods> goodsList = orderGoodsMapper.selectByOrderId(orderId);
            order.setOrderGoodsList(goodsList);
        }
        return order;
    }

    /**
     * 取消订单
     *
     * @param orderId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/cancel", method = RequestMethod.POST)
    @Transactional
    @Override
    public void cancel(Long orderId) {
        cancel(orderId, null, false);
    }

    private void cancel(Long orderId, String userId, boolean checkUser) {
        Order order = orderMapper.selectByPrimaryKey(orderId);
        if (order != null) {
            if (checkUser) {
                if (StringUtils.isEmpty(userId) || !userId.equals(order.getUserId())) {
                    throw new BusinessException("订单不存在");
                }
            }
        } else {
            throw new BusinessException("订单不存在");
        }
        order.setOrderStatus(OrderStatus.CANCELED.getCode());
        List<OrderGoods> orderGoodsList = orderGoodsMapper.selectByOrderId(orderId);
        for (OrderGoods orderGoods : orderGoodsList) {
            GoodsVo goodsVo = goodsDao.findOneById(orderGoods.getGoodsId());
            if (goodsVo != null) {
                List<SpecGoodsPrice> specGoodsPriceList = goodsVo.getSpecGoodsPriceList();
                specGoodsPriceList.forEach(x -> x.setStoreCount(x.getStoreCount() + orderGoods.getGoodsNum()));
                goodsVo.setSpecGoodsPriceList(specGoodsPriceList);
                goodsDao.save(goodsVo);
            }
        }
        orderMapper.updateByPrimaryKeySelective(order);
        //订单日志
        orderActionService.save(order, checkUser == true ? "取消订单" : "自动取消订单", userId);
    }

    /**
     * 自动取消订单
     *
     * @param orderId
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selfCancel", method = RequestMethod.POST)
    @Transactional
    @Override
    public void selfCancel(Long orderId, String userId) {
        cancel(orderId, userId, true);
    }

    /**
     * 取消过期订单
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/autoCancelExpiredOrder")
    @Transactional
    @Override
    public void autoCancelExpiredOrder() {
        List<Order> expiredOrderList = orderMapper.selectExpiredOrder(MallConstant.EXPIRED_TIME_INTERVAL);
        if (!CollectionUtils.isEmpty(expiredOrderList)) {
            for (Order order : expiredOrderList) {
                cancel(order.getOrderId());
            }
        }
    }

    /**
     * 查询各类型的订单
     *
     * @param type 0-全部订单，1-全部有效订单，2-待支付，3-待收货，4-已关闭
     * @return
     */
    //@RequestMapping(value = "/queryOrderNum", method = RequestMethod.POST)
    @Override
    public Integer queryOrderNum(Integer type, String userId) {
        return orderMapper.selectOrderNum(type, userId);
    }

    /**
     * 确认收货
     *
     * @param orderId
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/confirmReceiveGoods", method = RequestMethod.POST)
    @Transactional
    @Override
    public void confirmReceiveGoods(Long orderId, String userId) {
        Order order = orderMapper.selectByPrimaryKey(orderId);
        if (order == null) {
            throw new BusinessException("订单不存在");
        } else {
            if (StringUtils.isEmpty(userId) || !userId.equals(order.getUserId())) {
                throw new BusinessException("订单不存在");
            }
        }
        order.setOrderStatus(OrderStatus.RECEIVED.getCode());
        order.setReceiveTime(System.currentTimeMillis());
        List<OrderGoods> orderGoodsList = orderGoodsMapper.selectByOrderId(orderId);
        for (OrderGoods orderGoods : orderGoodsList) {
            GoodsVo goodsVo = goodsDao.findOneById(orderGoods.getGoodsId());
            if (goodsVo != null) {
                List<SpecGoodsPrice> specGoodsPriceList = goodsVo.getSpecGoodsPriceList();
                specGoodsPriceList.forEach(x -> x.setStoreCount(x.getStoreCount() + orderGoods.getGoodsNum()));
                goodsVo.setSpecGoodsPriceList(specGoodsPriceList);
                goodsDao.save(goodsVo);
            }
        }
        orderMapper.updateByPrimaryKeySelective(order);
        //订单日志
        orderActionService.save(order, "确认收货", userId);
    }

    private Integer getOrderStatusByType(Integer type) {
        Integer orderStatus = null;
        if (type == 0) {
            orderStatus = null;
        } else if (type == 1) {
            orderStatus = 99;
        } else if (type == 2) {
            orderStatus = OrderStatus.CONFIRMED.getCode();
        } else if (type == 3) {
            orderStatus = OrderStatus.CONFIRMED.getCode();
        } else if (type == 4) {
            orderStatus = OrderStatus.CANCELED.getCode();
        }
        return orderStatus;
    }

    private Integer getPayStatusByType(Integer type) {
        Integer payStatus = null;
        if (type == 0) {
            payStatus = null;
        } else if (type == 1) {
            payStatus = null;
        } else if (type == 2) {
            payStatus = PayStatus.UNPAID.getCode();
        } else if (type == 3) {
            payStatus = PayStatus.PAID.getCode();
        } else if (type == 4) {
            payStatus = null;
        }
        return payStatus;
    }

    /**
     * 查询分页订单
     *
     * @param type
     * @param keywords
     * @param userId
     * @param addTime
     * @param pageSize
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPage", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public List<Order> queryByPage(Integer type, String keywords, String userId, Long addTime, int pageSize) {
        List<Order> pageList = orderMapper.queryByPage(type, keywords, userId, addTime == 0 ? "" : addTime, pageSize);
        return pageList;
    }

    /**
     * seata修改订单状态
     *
     * @param order
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateOrder", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public void updateOrder(Order order) {
        String sql = "update tp_order_kill set pay_status = ?,pay_code = ?,pay_name = ?,pay_time = ? where order_id = ?";
        jdbcTemplate.update(sql, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setInt(1, order.getPayStatus());
                ps.setString(2, order.getPayCode());
                ps.setString(3, order.getPayName());
                ps.setLong(4, order.getPayTime());
                ps.setLong(5, order.getOrderId());
            }
        });
//        orderMapper.updateByPrimaryKeySelective(order);
//        throw new RuntimeException("秒杀订单异常！");
    }
}
