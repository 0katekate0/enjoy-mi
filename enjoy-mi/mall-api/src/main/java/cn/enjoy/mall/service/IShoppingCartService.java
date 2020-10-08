package cn.enjoy.mall.service;

import cn.enjoy.mall.vo.ShoppingGoodsVo;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@RequestMapping("/order/mall/service/IShoppingCartService")
public interface IShoppingCartService {
    static final Integer SAVE_MODE_APPEND = 0;
    static final Integer SAVE_MODE_UPDATE = 1;

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    void save(@RequestParam("specGoodsId") Integer specGoodsId, @RequestParam("num") Integer num,
              @RequestParam("userId") String userId, @RequestParam("saveMode") Integer saveMode);

    @RequestMapping(value = "/remove", method = RequestMethod.POST)
    void remove(@RequestParam("specGoodsId") Integer specGoodsId, @RequestParam("userId") String userId);

    @RequestMapping(value = "/removeAll", method = RequestMethod.POST)
    void removeAll(@RequestParam("userId") String userId);

    @RequestMapping(value = "/list", method = RequestMethod.POST)
    List<ShoppingGoodsVo> list(@RequestParam("userId") String userId);

    @RequestMapping(value = "/findBySpecGoodsId", method = RequestMethod.POST)
    ShoppingGoodsVo findBySpecGoodsId(@RequestParam("specGoodsId") Integer specGoodsId, @RequestParam("userId") String userId);

    @RequestMapping(value = "/updateStatus", method = RequestMethod.POST)
    void updateStatus(@RequestParam("specGoodsIds") String specGoodsIds, @RequestParam("status") Integer status, @RequestParam("userId") String userId);

    @RequestMapping(value = "/findCheckedGoodsList", method = RequestMethod.POST)
    List<ShoppingGoodsVo> findCheckedGoodsList(@RequestParam("userId") String userId);

    @RequestMapping(value = "/removeCheckedGoodsList", method = RequestMethod.POST)
    void removeCheckedGoodsList(@RequestParam("userId") String userId);
}
