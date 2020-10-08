package cn.enjoy.mall.web.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.service.IGoodsService;
import cn.enjoy.sys.controller.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 商品相关
 */
@RestController
@RequestMapping("/api/goods")
public class GoodsContoller extends BaseController {
    @Autowired
    private IGoodsService goodsService;

    /**
     * 根据关键字搜索商品
     * @param type
     * @param brand
     * @param order
     * @param param
     * @param page
     * @param pageSize
     * @return
     */
    @GetMapping("searchList")
    public HttpResponseBody searchList(Integer type, Integer brand, String order, String param, int page, int pageSize){
        return HttpResponseBody.successResponse("ok",  goodsService.searchList(type,brand,order,param,page,pageSize));
    }

    /**
     * 商品详情
     * @param goodsId
     * @return
     */
    @GetMapping("/detail/{goodsId}")
    public HttpResponseBody detail(@PathVariable("goodsId") Integer goodsId){
        return HttpResponseBody.successResponse("ok",  goodsService.findOneById(goodsId));
    }

    /**
     * 商品详情
     * @param specGoodsId
     * @return
     */
    @GetMapping("/detailBySpecGoodsId/{specGoodsId}")
    public HttpResponseBody detailBySpecGoodsId(@PathVariable("specGoodsId") Integer specGoodsId){
        return HttpResponseBody.successResponse("ok",  goodsService.findOneBySpecGoodsId(specGoodsId));
    }

    /**
     * 查询热门商品
     * @param showNum
     * @return
     */
    @GetMapping("/hotSelling")
    public HttpResponseBody queryHotSelling(Integer showNum){
        return HttpResponseBody.successResponse("ok",  goodsService.queryHotSellingGoods(showNum));
    }



}
