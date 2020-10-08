package cn.enjoy.mall.service.impl.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.constant.OrderStatus;
import cn.enjoy.mall.constant.PayStatus;
import cn.enjoy.mall.constant.ShippingStatus;
import cn.enjoy.mall.dao.OrderGoodsMapper;
import cn.enjoy.mall.dao.OrderManageMapper;
import cn.enjoy.mall.dao.OrderMapper;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderGoods;
import cn.enjoy.mall.service.manage.IKillOrderManageService;
import cn.enjoy.mall.service.manage.IOrderManageService;
import cn.enjoy.mall.vo.OrderVo;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.*;

/**
 * 订单管理
 *
 * @author Jack
 * @date 2018/3/8.
 */
@RestController
//@RequestMapping("/order/mall/service/manage/IOrderManageService")
public class OrderManageServiceImpl implements IOrderManageService {


    @Resource
    private OrderMapper orderMapper;
    @Resource
    private OrderManageMapper orderManageMapper;
    @Resource
    private OrderGoodsMapper orderGoodsMapper;

    @Autowired
    private IKillOrderManageService iKillOrderManageService;

    @Autowired
    private RedisTemplate redisTemplate;

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
    public GridModel<OrderVo> queryByPage(int page, int pageSize, OrderVo params,String userId) {
        Map map = firstQuery(page, pageSize, params,userId);
        Long total = (Long)map.get("total");
        List rows = (List)map.get("rows");
        GridModel<OrderVo> gm = new GridModel<>();
        gm.setRecords(total);
        gm.setRows(rows);
        gm.setPage(page);
        gm.setTotal(pageSize);
        return gm;
    }

    //第一次查询
    private Map firstQuery(int page, int pageSize, OrderVo params,String userId) {
        Map pageMap = new HashMap();
        //秒杀订单第一次查询
        JSONObject killMap = iKillOrderManageService.firstQuery(page, pageSize, params);
        List<OrderVo> killOrderVos = JSONArray.parseArray(JSON.toJSON(killMap.get("rows")).toString(),OrderVo.class);
        Long killtotal = Long.valueOf(killMap.get("total").toString());
        if(killOrderVos == null || killOrderVos.size() == 0) {
            //查询记录总量，分页用
            Long total = orderManageMapper.queryByPageCount(params);
            Long naddTime = 0L;
            Long kaddTime = 0L;
            //获取前一页的普通订单的最大时间记录
            if (redisTemplate.opsForHash().hasKey(userId, "mng" + page)) {
                Map<String, Long> addTimeMap = (Map) redisTemplate.opsForHash().get(userId, "mng" + page);
                naddTime = addTimeMap.get("n") == null ? 0L : addTimeMap.get("n");
                kaddTime = addTimeMap.get("k") == null ? 0L : addTimeMap.get("k");
            }
            params.setAddTime(naddTime);

            params.setOffset(0);
            params.setLimit(pageSize);
            PageList<OrderVo> orderVos = orderManageMapper.queryByPage(params);
            pageMap.put("total",killtotal + total);
            pageMap.put("rows",orderVos);
            orderMaxTime(orderVos,userId,page);
            return pageMap;
        }

        //1、计算第一次查询的offset
        int offset = (page - 1) * pageSize / 2;
        params.setOffset(offset);
        params.setLimit(pageSize);
        PageList<OrderVo> orderVos = orderManageMapper.queryByPage(params);

        //当点击最后一页的时候，或者倒数几页的时候，可能会存在某一个库已经没有数据的情况
        //这时候的处理是要查询另一个库接下来的offset的pageSize条数据
        if(orderVos == null || orderVos.size() == 0) {
            Long naddTime = 0L;
            Long kaddTime = 0L;
            //获取前一页的秒杀订单的最大时间记录
            if (redisTemplate.opsForHash().hasKey(userId, "mng" + page)) {
                Map<String, Long> addTimeMap = (Map) redisTemplate.opsForHash().get(userId, "mng" + page);
                naddTime = addTimeMap.get("n") == null ? 0L : addTimeMap.get("n");
                kaddTime = addTimeMap.get("k") == null ? 0L : addTimeMap.get("k");
            }
            params.setAddTime(kaddTime);
            JSONObject killjo = iKillOrderManageService.queryByPage(page, pageSize, params);
            List<OrderVo> allkillOrderVos = JSONArray.parseArray(JSON.toJSON(killjo.get("rows")).toString(),OrderVo.class);
            Long allkilltotal = Long.valueOf(killjo.get("total").toString());
            pageMap.put("total",orderManageMapper.queryByPageCount(params) + allkilltotal);
            pageMap.put("rows",allkillOrderVos);
            killOrderMaxTime(allkillOrderVos,userId,page);
            return pageMap;
        }

        //如果秒杀订单和普通订单都有数据的情况
        PageList<OrderVo> realResult = secondQuery(orderVos, killOrderVos, offset, page, pageSize,userId);
        pageMap.put("rows",realResult);
        pageMap.put("total",killtotal + orderManageMapper.queryByPageCount(params));
        return pageMap;
    }

    //记录秒杀订单的上一页的最大订单记录的最大时间
    private void killOrderMaxTime(List<OrderVo> allkillOrderVos,String userId,int page) {
        Map<String, Long> addTimeMap = new HashMap<>();
        addTimeMap.put("k", allkillOrderVos.get(allkillOrderVos.size() - 1).getAddTime());
        redisTemplate.opsForHash().putIfAbsent(userId, "mng" + (page + 1), addTimeMap);
    }

    //记录普通订单的上一页的最大订单记录的最大时间
    private void orderMaxTime(List<OrderVo> allOrderVos,String userId,int page) {
        Map<String, Long> addTimeMap = new HashMap<>();
        addTimeMap.put("n", allOrderVos.get(allOrderVos.size() - 1).getAddTime());
        redisTemplate.opsForHash().putIfAbsent(userId, "mng" + (page + 1), addTimeMap);
    }

    //第二次查询
    private PageList<OrderVo> secondQuery(PageList<OrderVo> orderVos, List<OrderVo> killOrderVos, int offset, int page, int pageSize,String userId) {
        //1、计算普通订单和秒杀订单第一次查询的最小时间
        Long minTime = orderVos.get(0).getAddTime() < killOrderVos.get(0).getAddTime() ? orderVos.get(0).getAddTime() :
                killOrderVos.get(0).getAddTime();

        //普通订单第二次查询
        //addTimeMin and addTimeMax
        PageList<OrderVo> seorderVos = orderManageMapper.secondQuerySql(minTime, orderVos.get(orderVos.size() - 1).getAddTime());
        PageList<OrderVo> sekillOrderVos = iKillOrderManageService.secondQuery(minTime, killOrderVos.get(killOrderVos.size() - 1).getAddTime());

        //在这里就确定了一个两个库第一次查询的最小值在两个表中的一个唯一offset坐标，根据这个坐标作为参照算出各个数据的offset
        int globalOffset = calculationGlobalOffset(orderVos, killOrderVos, seorderVos, sekillOrderVos, minTime, offset);
        PageList<OrderVo> rightResultSet = findRightResultSet(seorderVos, sekillOrderVos, globalOffset, page, pageSize, minTime,userId);
        return rightResultSet;
    }

    private PageList<OrderVo> findRightResultSet(PageList<OrderVo> seorderVos, PageList<OrderVo> sekillOrderVos, int globalOffset, int page, int pageSize, Long minTime,String userId) {
        List<OrderVo> collaseResult = new ArrayList<>();
        collaseResult.addAll(seorderVos);
        collaseResult.addAll(sekillOrderVos);

        //升序排序
        collaseResult.sort((x, y) -> x.getAddTime() > y.getAddTime() ? 1 : -1);

        int globalTargetOffset = (page - 1) * pageSize;

        int temp = globalOffset;
        boolean findit = false;

        //如果是第一页的情况 即offset=0
        if(globalOffset == globalTargetOffset && globalOffset == 0) {
            return new PageList<>(collaseResult.subList(0,collaseResult.size() < pageSize ? collaseResult.size() : pageSize));
        }

        List<OrderVo> realList = new ArrayList<>();
        //获取最小time对应的globalOffset所在list的index
        for (OrderVo orderVo : collaseResult) {
            //如果找到了最小时间的项
            if (findit) {
                temp++;
                if (temp >= (globalTargetOffset + 1)) {
                    realList.add(orderVo);
                    if(realList.size() == pageSize) break;
                }
                continue;
            }

            if (minTime.toString().equals(orderVo.getAddTime().toString())) {
                findit = true;
            }
        }

        flagMaxTime(page,realList,seorderVos,sekillOrderVos,userId);
        //获取事件分页的offset所在list的index
        return new PageList<>(realList);
    }

    //记录一下每一次分页的最大时间
    private void flagMaxTime(int page,List<OrderVo> realList,PageList<OrderVo> seorderVos, PageList<OrderVo> sekillOrderVos,String userId) {
        Long naddTime = null;
        Long kaddTime = null;
        Collections.reverse(realList);
        for (OrderVo orderVo : realList) {
            if(seorderVos.contains(orderVo)) {
                naddTime = orderVo.getAddTime();
                break;
            }
        }
        for (OrderVo orderVo : realList) {
            if(sekillOrderVos.contains(orderVo)) {
                kaddTime = orderVo.getAddTime();
                break;
            }
        }
        //记录下一页的时间基准线，下一页的时间要小于这个基准线
        Map<String, Long> addTimeMap = new HashMap<>();
        addTimeMap.put("n", naddTime);
        addTimeMap.put("k", kaddTime);
        redisTemplate.opsForHash().putIfAbsent(userId, "mng" + (page + 1), addTimeMap);
    }

    private int calculationGlobalOffset(PageList<OrderVo> orderVos, List<OrderVo> killOrderVos, PageList<OrderVo> seorderVos, PageList<OrderVo> sekillOrderVos, Long minTime, int offset) {
        int orderOffset = 0;
        int killorderOffset = 0;
        //1、判断最小time是在哪一个库中
        //如果第二次查询的结果中，第一个值就是算出来的最小值，则说明，最小值来自这个库的第二次查询，那么offset就会第一个值的offset
        if (seorderVos.get(0).getAddTime().toString().equals(minTime.toString())) {
            orderOffset = offset;
        } else {
            //如果不相等，计算offset ，计算第一次，第二次查询的差值
            //计算 ，两个库的最小值和该库的第一次查询的最小值之间. 第二次查询时多出几条数据
            int i = 0;
            for (OrderVo seorderVo : seorderVos) {
                //这里如果查询出来的时间有相等的如何处理？？
                if (seorderVo.getAddTime() < orderVos.get(0).getAddTime() && seorderVo.getAddTime() > minTime) {
                    i++;
                }
            }
            //比如中间有i条记录，那么minTime所在的位置，就是在i条记录之前的第一个位置
            orderOffset = offset == 0 ? 0 : offset - i - 1;
        }

        //可能会存在，minTime两个库都是在第一条的情况
        if (sekillOrderVos.get(0).getAddTime().toString().equals(minTime.toString())) {
            killorderOffset = offset;
        } else {
            //如果不相等，计算offset ，计算第一次，第二次查询的差值
            //计算 ，两个库的最小值和该库的第一次查询的最小值之间. 第二次查询时多出几条数据
            int i = 0;
            for (OrderVo sekillorderVo : sekillOrderVos) {
                //这里如果查询出来的时间有相等的如何处理？？
                if (sekillorderVo.getAddTime() < killOrderVos.get(0).getAddTime() && sekillorderVo.getAddTime() > minTime) {
                    i++;
                }
            }
            //比如中间有i条记录，那么minTime所在的位置，就是在i条记录之前的第一个位置
            killorderOffset = offset == 0 ? 0 : offset - i - 1;
        }
        return orderOffset + killorderOffset;
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
