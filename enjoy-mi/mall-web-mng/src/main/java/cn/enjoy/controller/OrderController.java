package cn.enjoy.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.model.DeliveryDoc;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.service.IOrderActionService;
import cn.enjoy.mall.service.IOrderService;
import cn.enjoy.mall.service.manage.IDeliveryService;
import cn.enjoy.mall.service.manage.IOrderManageService;
import cn.enjoy.mall.service.manage.IShippingService;
import cn.enjoy.mall.vo.OrderCreateVo;
import cn.enjoy.mall.vo.OrderVo;
import cn.enjoy.sys.controller.BaseController;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;


/**
 * 订单管理
 *
 * @author Jack
 * @date 2020/9/7
 */
@RestController
@RequestMapping("/api/order")
public class OrderController extends BaseController {
    @Autowired
    private IOrderService orderService;
    @Autowired
    private IOrderManageService orderManageService;
    @Autowired
    private IOrderActionService orderActionService;
    @Autowired
    private IShippingService shippingService;
    @Autowired
    private IDeliveryService deliveryService;

    /**
     * 创建订单
     *
     * @param crderCreateVo
     * @return
     */
    @PostMapping("save")
    public HttpResponseBody save(@RequestBody OrderCreateVo crderCreateVo) {
        Long orderId = orderService.createOrder(crderCreateVo, getSessionUserId());
        return HttpResponseBody.successResponse("ok", orderId);
    }

    /**
     * 分页查询订单
     *
     * @param page
     * @param pageSize
     * @return
     */
    @GetMapping("queryByPage")
    public HttpResponseBody queryByPage(
            @RequestParam(required = false, defaultValue = "0") int page,
            @RequestParam(required = false, defaultValue = "10") int pageSize,
            OrderVo orderVo) {
        return HttpResponseBody.successResponse("ok",
                orderManageService.queryByPage(page, pageSize, orderVo,getSessionUserId()));
    }

    /**
     * 查询订单详细信息
     *
     * @param orderId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @GetMapping("detail/{orderId}")
    public HttpResponseBody detail(@PathVariable("orderId") Long orderId) {
        return HttpResponseBody.successResponse("ok",
                orderManageService.queryOrderDetail(orderId));
    }


    /**
     * 更新订单信息
     *
     * @param order
     * @param action
     * @param remark
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @PostMapping("update")
    public HttpResponseBody update(Order order, String action, String remark) {
        orderManageService.update(order);
        Order newOrder = orderManageService.queryOrderDetail(order.getOrderId());
        orderActionService.save(newOrder, action, this.getSessionUserId(), remark);
        return HttpResponseBody.successResponse("操作成功");
    }

    /**
     * 快递查询
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @GetMapping("queryShipping")
    public HttpResponseBody queryShipping() {
        return HttpResponseBody.successResponse("ok", shippingService.queryAll());
    }

    /**
     * 根据订单ID查询发货信息
     *
     * @param orderId
     * @return
     */
    @GetMapping("queryDeliveryDocByOrderId")
    public HttpResponseBody queryDeliveryDocByOrderId(Long orderId) {
        return HttpResponseBody.successResponse("ok", deliveryService.queryDeliveryDocByOrderId(orderId));
    }

    /**
     * 快递信息保存
     *
     * @param deliveryDoc
     * @param selectedIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @PostMapping("shipping")
    public HttpResponseBody shipping(DeliveryDoc deliveryDoc, Long[] selectedIds) {
        deliveryDoc.setAdminId(this.getSessionUserId());
        deliveryService.shipping(JSONObject.toJSONString(deliveryDoc), Arrays.asList(selectedIds));

        return HttpResponseBody.successResponse("操作成功");
    }

    /**
     * 根据订单id查询商品信息
     *
     * @param orderId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @GetMapping("queryGoodsByOrderId")
    public HttpResponseBody queryGoodsByOrderId(Long orderId) {
        return HttpResponseBody.successResponse("ok", orderManageService.selectGoodsByOrderId(orderId));
    }

    /**
     * 根据订单id查询订单日志信息
     * @param orderId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @GetMapping("queryLogByOrderId")
    public HttpResponseBody queryLogByOrderId(Long orderId) {
        return HttpResponseBody.successResponse("ok", orderActionService.queryByOrderId(orderId));
    }


    /**
     * 取消订单
     *
     * @param orderId
     * @return
     */
    @PostMapping("cancel")
    public HttpResponseBody cancel(@RequestParam Long orderId) {
        orderService.selfCancel(orderId, getSessionUserId());
        return HttpResponseBody.successResponse("ok");
    }

    /**
     * 确认收货
     *
     * @param orderId
     * @return
     */
    @PostMapping("confirmReceiveGoods")
    public HttpResponseBody confirmReceiveGoods(@RequestParam Long orderId) {
        orderService.confirmReceiveGoods(orderId, getSessionUserId());
        return HttpResponseBody.successResponse("ok");
    }

    /**
     * 查询各状态的订单数
     *
     * @param type 0-全部订单，1-全部有效订单，2-待支付，3-待收货，4-已关闭
     * @return
     */
    @GetMapping("queryOrderNum")
    public HttpResponseBody queryOrderNum(@RequestParam(required = false, defaultValue = "") Integer type) {
        return HttpResponseBody.successResponse("ok", orderService.queryOrderNum(type, getSessionUserId()));
    }

}
