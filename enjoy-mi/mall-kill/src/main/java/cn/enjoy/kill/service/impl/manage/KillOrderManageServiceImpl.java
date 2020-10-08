package cn.enjoy.kill.service.impl.manage;

import cn.enjoy.kill.dao.OrderGoodsMapper;
import cn.enjoy.kill.dao.OrderManageMapper;
import cn.enjoy.kill.dao.OrderMapper;
import cn.enjoy.mall.constant.OrderStatus;
import cn.enjoy.mall.constant.PayStatus;
import cn.enjoy.mall.constant.ShippingStatus;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderGoods;
import cn.enjoy.mall.service.manage.IKillOrderManageService;
import cn.enjoy.mall.vo.OrderVo;
import com.alibaba.fastjson.JSONObject;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * 订单管理
 *
 * @author Jack
 * @date 2018/3/8.
 */
@RestController
//@RequestMapping("/order/mall/service/manage/IOrderManageService")
public class KillOrderManageServiceImpl implements IKillOrderManageService {


    @Resource
    private OrderMapper orderMapper;
    @Resource
    private OrderManageMapper orderManageMapper;
    @Resource
    private OrderGoodsMapper orderGoodsMapper;


    /**
     * 查询订单列表
     *
     * @param page
     * @param pageSize
     * @param params
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    @Override
    public JSONObject queryByPage(int page, int pageSize, OrderVo params) {
        JSONObject pageMap = new JSONObject();
        params.setOffset(0);
        params.setLimit(pageSize);
        PageList<OrderVo> orderVos = orderManageMapper.queryByPage(params);
        pageMap.put("rows",orderVos);
        pageMap.put("total",orderManageMapper.queryByPageCount(params));
        return pageMap;
    }

    @Override
    //第一次查询
    public JSONObject firstQuery(int page, int pageSize, OrderVo params) {
        JSONObject pageMap = new JSONObject();
        //1、计算第一次查询的offset
        int offset = (page - 1) * pageSize / 2;
        params.setOffset(offset);
        params.setLimit(pageSize);
        PageList<OrderVo> orderVos = orderManageMapper.queryByPage(params);
        pageMap.put("rows",JSONObject.toJSONString(orderVos));
        pageMap.put("total",orderManageMapper.queryByPageCount(params));
        return pageMap;
    }

    @Override
    public PageList<OrderVo> secondQuery(Long addTimeMin, Long addTimeMax) {
        return orderManageMapper.secondQuerySql(addTimeMin,addTimeMax);
    }

    /**
     * 查询订单详情
     *
     * @param orderId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryOrderDetail", method = RequestMethod.POST)
    @Override
    public OrderVo queryOrderDetail(Long orderId) {
        return orderMapper.selectOrderById(orderId);
    }

    @Override
    public void save(OrderVo orderVo) {

    }

    @Override
    public void delete(short id) {

    }

    @Override
    public void deleteByIds(String[] ids) {

    }

    /**
     * 查询订单商品
     *
     * @param orderId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectGoodsByOrderId", method = RequestMethod.POST)
    @Override
    public List<OrderGoods> selectGoodsByOrderId(Long orderId) {
        return orderGoodsMapper.selectByOrderId(orderId);
    }

    /**
     * 修改订单记录
     *
     * @param order
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/update", method = RequestMethod.POST)
    @Override
    public int update(Order order) {
        if (OrderStatus.CONFIRMED.getCode().equals(order.getOrderStatus())) {
            order.setConfirmTime(System.currentTimeMillis() / 1000);
        }
        if (PayStatus.PAID.getCode().equals(order.getPayStatus())) {
            order.setPayTime(System.currentTimeMillis() / 1000);
        }
        if (ShippingStatus.SHIPPED.getCode().equals(order.getShippingStatus())) {
            order.setShippingTime(System.currentTimeMillis() / 1000);
        }
        return orderMapper.updateByPrimaryKeySelective(order);
    }
}
