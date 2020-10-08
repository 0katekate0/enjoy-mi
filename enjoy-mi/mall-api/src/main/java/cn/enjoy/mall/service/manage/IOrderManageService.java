package cn.enjoy.mall.service.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderGoods;
import cn.enjoy.mall.vo.OrderVo;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

/**
 * @author Ray
 * @date 2018/3/7.
 */
@RequestMapping("/order/mall/service/manage/IOrderManageService")
public interface IOrderManageService {

    @RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    GridModel<OrderVo> queryByPage(@RequestParam("page") int page,
                                   @RequestParam("pageSize") int pageSize,
                                   @RequestBody OrderVo params,
                                   @RequestParam("userId") String userId);

    @RequestMapping(value = "/queryOrderDetail", method = RequestMethod.POST)
    OrderVo queryOrderDetail(@RequestParam("orderId") Long orderId);

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    void save(@RequestBody OrderVo orderVo);

    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    void delete(@RequestParam("id") short id);

    @RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
    void deleteByIds(@RequestBody String[] ids);

    @RequestMapping(value = "/selectGoodsByOrderId", method = RequestMethod.POST)
    List<OrderGoods> selectGoodsByOrderId(@RequestParam("orderId") Long orderId);

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    int update(@RequestBody Order order);

}
