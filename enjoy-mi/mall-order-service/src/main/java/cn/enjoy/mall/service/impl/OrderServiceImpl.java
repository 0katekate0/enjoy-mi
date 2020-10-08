package cn.enjoy.mall.service.impl;

import cn.enjoy.core.exception.BusinessException;
import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.constant.MallConstant;
import cn.enjoy.mall.constant.OrderStatus;
import cn.enjoy.mall.constant.PayStatus;
import cn.enjoy.mall.constant.ShippingStatus;
import cn.enjoy.mall.dao.OrderGoodsMapper;
import cn.enjoy.mall.dao.OrderMapper;
import cn.enjoy.mall.dao.UserAddressMapper;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderGoods;
import cn.enjoy.mall.model.SpecGoodsPrice;
import cn.enjoy.mall.model.UserAddress;
import cn.enjoy.mall.mongo.GoodsDao;
import cn.enjoy.mall.service.*;
import cn.enjoy.mall.vo.GoodsVo;
import cn.enjoy.mall.vo.OrderCreateVo;
import cn.enjoy.mall.vo.ShoppingGoodsVo;
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
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 订单管理
 * 0|0|0 待支付
 * 0|0|1 已付款待配货
 * 1|0|1 已配货待出库
 * 1|1|1 待收货
 * 2|1|1 已完成
 * 4|1|1 已完成
 *
 * @author Jack
 */
@Slf4j
@RestController
//@RequestMapping("/order/mall/service/IOrderService")
public class OrderServiceImpl implements IOrderService {
    @Resource
    private OrderMapper orderMapper;
    @Resource
    private OrderGoodsMapper orderGoodsMapper;
    @Resource
    private IShoppingCartService shoppingCartService;
    @Resource
    private GoodsDao goodsDao;
    @Resource
    private IOrderActionService orderActionService;
    @Resource
    private SequenceGenerator sequenceGenerator;

    @Autowired
    private DefaultUidGenerator defaultUidGenerator;

    @Autowired
    private CachedUidGenerator cachedUidGenerator;

    @Autowired
    private IUserAddressService iUserAddressService;

    @Autowired
    private UserAddressMapper userAddressMapper;

    @Autowired
    private IKillOrderService iKillOrderService;

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 根据用户id查询订单
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
        //做路由转换
//        map.put("orderType","K");
        map.put("userId", userId);
        return orderMapper.queryByPage(map);
    }

    /**
     * 新建订单记录
     *
     * @param orderCreateVo
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/createOrder", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Transactional
    @Override
    public Long createOrder(OrderCreateVo orderCreateVo, String userId) {
        //创建一个订单
        Order order = new Order();
        order.setOrderId(defaultUidGenerator.getUID());
        order.setOrderType("P");
        //从SequenceGenerator中获取订单的变化
        order.setOrderSn(sequenceGenerator.getOrderNo());
        order.setAddTime(System.currentTimeMillis());
        //设置订单的状态为未确定订单
        order.setOrderStatus(OrderStatus.UNCONFIRMED.getCode());
        //未支付
        order.setPayStatus(PayStatus.UNPAID.getCode());
        //未发货
        order.setShippingStatus(ShippingStatus.UNSHIPPED.getCode());
        //获取发货地址
        UserAddress userAddress = userAddressMapper.selectByPrimaryKey(orderCreateVo.getAddressId());
        BeanUtils.copyProperties(userAddress, order);
        order.setUserId(userId);

        //新增订单
        orderMapper.insert(order);
        Long orderId = order.getOrderId();
        BigDecimal totalAmount = new BigDecimal(0);
        //从mongodb的购物车中获取所购物品
        List<ShoppingGoodsVo> checkedGoodsList = shoppingCartService.findCheckedGoodsList(userId);
        List<OrderGoods> orderGoodsList = new ArrayList<>();
        for (ShoppingGoodsVo goodsAddVo : checkedGoodsList) {
            GoodsVo goodsVo = goodsDao.findOneBySpecGoodsId(goodsAddVo.getSpecGoodsId());
            if (goodsVo == null) {
                throw new BusinessException("没有找到对应的商品[" + goodsAddVo.getGoodsName() + "],可能已下架");
            }
            if (goodsVo.getBase().getIsOnSale() == false) {
                throw new BusinessException("对不起，商品[" + goodsAddVo.getGoodsName() + "]已下架");
            }
            List<SpecGoodsPrice> machedSpecGoodsPriceList = goodsVo.getSpecGoodsPriceList().stream()
                    .filter(specGoodsPrice -> specGoodsPrice.getId().intValue() == goodsAddVo.getSpecGoodsId().intValue()).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(machedSpecGoodsPriceList)) {
                throw new BusinessException("没有找到对应的商品[" + goodsAddVo.getGoodsName() + "],可能已下架");
            }
            SpecGoodsPrice specGoodsPrice = machedSpecGoodsPriceList.get(0);
            //更新商品库存
            goodsVo.getSpecGoodsPriceList().remove(specGoodsPrice);
            specGoodsPrice.setStoreCount(specGoodsPrice.getStoreCount() - goodsAddVo.getNum().shortValue());
            goodsVo.getSpecGoodsPriceList().add(specGoodsPrice);
            //把当前订单信息保存到mongodb
            goodsDao.save(goodsVo);

            //创建的订单商品
            OrderGoods orderGoods = new OrderGoods();
            orderGoods.setRecId(defaultUidGenerator.getUID());
            orderGoods.setOrderType("P");
            orderGoods.setOrderId(orderId);
            BeanUtils.copyProperties(goodsVo.getBase(), orderGoods);
            orderGoods.setGoodsNum(goodsAddVo.getNum().shortValue());
            orderGoods.setGoodsPrice(specGoodsPrice.getPrice());
            orderGoods.setBarCode(specGoodsPrice.getBarCode());
            orderGoods.setSpecKey(specGoodsPrice.getKey());
            orderGoods.setSpecKeyName(specGoodsPrice.getKeyName());
            orderGoods.setSpecGoodsId(specGoodsPrice.getId());
            if (CollectionUtils.isEmpty(specGoodsPrice.getSpecGoodsImagesList())) {
                orderGoods.setOriginalImg(goodsVo.getBase().getOriginalImg());
            } else {
                orderGoods.setOriginalImg(specGoodsPrice.getSpecGoodsImagesList().get(0).getSrc());
            }
            orderGoodsList.add(orderGoods);
            totalAmount = totalAmount.add(specGoodsPrice.getPrice().multiply(new BigDecimal(goodsAddVo.getNum())));
        }
        order.setGoodsPrice(totalAmount);
        order.setShippingPrice(new BigDecimal(0));
        order.setOrderAmount(totalAmount.add(order.getShippingPrice()));
        order.setTotalAmount(totalAmount.add(order.getShippingPrice()));

        //修改订单
        orderMapper.updateByPrimaryKeySelective(order);

        //保存订单产品信息
        orderGoodsMapper.insertBatch(orderGoodsList);
        //清除购物车中已下单的商品
        shoppingCartService.removeCheckedGoodsList(userId);
        //订单日志
        orderActionService.save(order, "创建订单", userId);

        if (redisTemplate.hasKey(userId)) {
            //清空用于分页的缓存
            redisTemplate.delete(userId);
        }

        return orderId;
    }

/*    @Transactional
    @Master
    public Long killOrder(int addressId, KillGoodsSpecPriceDetailVo killGoods, String userId) {
        //创建一个订单
        Order order = new Order();
        order.setOrderType("K");
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
        BeanUtils.copyProperties(userAddress,order);
        order.setUserId(userId);

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
        BeanUtils.copyProperties(goodsVo.getBase(),orderGoods);
        orderGoods.setPromType(true);
        orderGoods.setPromId(killGoods.getId());
        orderGoods.setGoodsNum((short)1);
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
        orderActionService.save(order,"创建秒杀订单",userId);

        return orderId;
    }*/

/*    @Transactional
    @Master
    @Override
    public Long killOrder(KillOrderVo killOrderVo) {
        return this.killOrder(killOrderVo.getAddressId(),
                killOrderVo.getKillGoodsSpecPriceDetailVo(),killOrderVo.getUserId());
    }*/


    /**
     * 分页查询订单
     *
     * @param type 0-全部订单，1-全部有效订单，2-待支付，3-待收货，4-已关闭
     * @param keywords 订单号
     * @param page
     * @param pageSize
     * @return
     */
    //@RequestMapping(value = "/searchListPage", method = RequestMethod.POST)
    @Override
    public GridModel<Order> searchListPage(Integer type, String keywords, int page, int pageSize, String userId) {
        //1、先获取普通订单和秒杀订单的最大的addTime时间
        Long naddTime = null;
        Long kaddTime = null;

        //如果是第一页查询，就是降序查询从最大的时间开始查
        if (page == 1) {
            naddTime = 0L;
            kaddTime = 0L;
        } else {
            if (redisTemplate.opsForHash().hasKey(userId, page)) {
                Map<String, Long> addTimeMap = (Map) redisTemplate.opsForHash().get(userId, page);
                naddTime = addTimeMap.get("n") == null ? 0L : addTimeMap.get("n");
                kaddTime = addTimeMap.get("k") == null ? 0L : addTimeMap.get("k");
            } else {
                //如果缓存没有显示第一页数据
                naddTime = 0L;
                kaddTime = 0L;
            }
        }

        List<Order> normalOrders = orderMapper.queryByPage(type, keywords, userId, naddTime == 0 ? "" : naddTime, pageSize);
        //调用秒杀系统的查询接口
        List<Order> killorders = iKillOrderService.queryByPage(type, keywords, userId, kaddTime, pageSize);

        //对普通订单和秒杀订单的合并排序并且记录页码和addTime的关系
        List<Order> pageList = sortOrdersByTime(normalOrders, killorders, userId, page, pageSize);
        return new GridModel<Order>(pageList);
    }

    private List<Order> sortOrdersByTime(List<Order> normalOrders, List<Order> killorders, String userId, int page, int pageSize) {
        List<Order> allOrders = new ArrayList();
        allOrders.addAll(normalOrders);
        allOrders.addAll(killorders);
        //这里是按照addTime时间降序排序
        allOrders.sort((x, y) -> x.getAddTime() > y.getAddTime() ? -1 : 1);

        List<Order> orders = allOrders.subList(0, allOrders.size() < pageSize ? allOrders.size() : pageSize);

        //深拷贝这个list
        List<Order> sortOrders = Arrays.asList(new Order[orders.size()]);
        Collections.copy(sortOrders, orders);

        //记录normalOrders，killorders最小的addTime  升续
        sortOrders.sort((x, y) -> x.getAddTime() > y.getAddTime() ? 1 : -1);

        Long naddTime = null;
        Long kaddTime = null;
        for (Order order : sortOrders) {
            if (normalOrders.contains(order)) {
                naddTime = order.getAddTime();
                break;
            }
        }
        for (Order order : sortOrders) {
            if (killorders.contains(order)) {
                kaddTime = order.getAddTime();
                break;
            }
        }
        //记录下一页的时间基准线，下一页的时间要小于这个基准线
        Map<String, Long> addTimeMap = new HashMap<>();
        addTimeMap.put("n", naddTime);
        addTimeMap.put("k", kaddTime);
        redisTemplate.opsForHash().putIfAbsent(userId, page + 1, addTimeMap);
        return orders;
    }

    /**
     * 查询订单详情
     *
     * @param orderId
     * @return
     */
    //@RequestMapping(value = "/selectOrderDetail", method = RequestMethod.POST)
    @Override
    public Order selectOrderDetail(Long orderId) {
        Order order = orderMapper.selectByPrimaryKey(orderId);
        if (order != null) {
            List<OrderGoods> goodsList = orderGoodsMapper.selectByOrderId(orderId);
            order.setOrderGoodsList(goodsList);
        }
        return order;
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
        } else {
            //如果订单查询不到，则可能是秒杀订单，去秒杀订单
            Order search = iKillOrderService.search(orderId);
            return search;
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
     * 自动取消过期订单
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
        Integer normalOrder = orderMapper.selectOrderNum(type, userId);
        Integer killOrder = iKillOrderService.queryOrderNum(type, userId);
        return normalOrder + killOrder;
    }

    /**
     * 确认收货接口
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
     * seata修改订单
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
        String sql = "update tp_order set pay_status = ?,pay_code = ?,pay_name = ?,pay_time = ? where order_id = ?";
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
    }
}
