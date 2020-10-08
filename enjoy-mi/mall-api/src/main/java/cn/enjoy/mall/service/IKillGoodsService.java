package cn.enjoy.mall.service;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.vo.KillGoodsSpecPriceDetailVo;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping("/mall/service/IKillGoodsService")
public interface IKillGoodsService {

    @RequestMapping(value = "/findOneByKillGoodsId", method = RequestMethod.POST)
    KillGoodsSpecPriceDetailVo findOneByKillGoodsId(@RequestParam("killGoodsId") Integer killGoodsId);

    @RequestMapping(value = "/searchList", method = RequestMethod.POST)
    GridModel<KillGoodsSpecPriceDetailVo> searchList(@RequestParam("page") int page, @RequestParam("pageSize") int pageSize);

    @RequestMapping(value = "/publishAll2MongoDB")
    void publishAll2MongoDB();

    @RequestMapping(value = "/publishGoods2MongoDB", method = RequestMethod.POST)
    void publishGoods2MongoDB(@RequestParam("killGoodsId") Integer killGoodsId);

}
