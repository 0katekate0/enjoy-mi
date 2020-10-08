package cn.enjoy.mall.service;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.model.*;
import cn.enjoy.mall.vo.GoodsDetailVo;
import cn.enjoy.mall.vo.GoodsPageVo;
import cn.enjoy.mall.vo.GoodsSpecPriceAttrVo;
import cn.enjoy.mall.vo.GoodsVo;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

/**
 * @author Ray
 * @date 2018/2/5.
 */
@RequestMapping("/product/mall/service/IGoodsService")
public interface IGoodsService {
    /**
     * 从mongodb中查找完整信息
     * @param goodsId
     * @return
     */
    @RequestMapping(value = "/findOneById", method = RequestMethod.POST)
    GoodsVo findOneById(@RequestParam("goodsId") Integer goodsId) ;

    /**
     * 从mongodb中查找完整信息
     * @param specGoodsId
     * @return
     */
    @RequestMapping(value = "/findOneBySpecGoodsId", method = RequestMethod.POST)
    GoodsVo findOneBySpecGoodsId(@RequestParam("specGoodsId") Integer specGoodsId) ;

    @RequestMapping(value = "/findOneByKillGoodsId", method = RequestMethod.POST)
    GoodsVo findOneByKillGoodsId(@RequestParam("killGoodsId") Integer killGoodsId);

    /**
     * 从mongodb中分页查找
     * @param catId
     * @param brandId
     * @param order
     * @param keyword
     * @param page
     * @param pageSize
     * @return
     */
    @RequestMapping(value = "/searchList", method = RequestMethod.POST)
    GridModel<GoodsPageVo> searchList(@RequestParam("catId") Integer catId, @RequestParam("brandId") Integer brandId,
                                      @RequestParam("order") String order, @RequestParam("keyword") String keyword,
                                      @RequestParam("page") int page, @RequestParam("pageSize") int pageSize);

    /**
     * 保存到数据库
     * @param goods
     * @return
     */
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    int save(@RequestBody Goods goods);

    /**
     * 从数据库根据id删除
     * @param goodsId
     * @return
     */
    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    int delete(@RequestParam("goodsId") Integer goodsId);

    /**
     * 从数据库根据id批量删除
     * @param goodsIds
     * @return
     */
    @RequestMapping(value = "/deteteBatch", method = RequestMethod.POST)
    int deteteBatch(@RequestBody Integer[] goodsIds);

    /**
     * 从数据库分页查询列表
     * @param keyword
     * @param page
     * @param pageSize
     * @return
     */
    @RequestMapping(value = "/queryListPageFromDB", method = RequestMethod.POST)
    GridModel<Goods> queryListPageFromDB(@RequestParam(value = "catId",required = false) Integer catId, @RequestParam(value = "keyword",required = false) String keyword,
                                            @RequestParam(value = "page",required = false) int page, @RequestParam(value = "pageSize",required = false) int pageSize,
                                            @RequestParam(value = "sidx",required = false) String sidx, @RequestParam(value = "sord",required = false) String sord);

    /**
     * 从数据库查询商品详细信息
     * @param goodsId
     * @return
     */
    @RequestMapping(value = "/queryDetailFromDB", method = RequestMethod.POST)
    GoodsDetailVo queryDetailFromDB(@RequestParam("goodsId") Integer goodsId);

    /**
     * 从数据库查询商品信息
     * @param goodsId
     * @return
     */
    @RequestMapping(value = "/queryGoodsFromDB", method = RequestMethod.POST)
    Goods queryGoodsFromDB(@RequestParam("goodsId") Integer goodsId);

    /**
     * 从数据库查询商品图片
     * @param goodsId
     * @return
     */
    @RequestMapping(value = "/queryGoodsImageFromDB", method = RequestMethod.POST)
    List<GoodsImages> queryGoodsImageFromDB(@RequestParam("goodsId") Integer goodsId);

    /**
     * 从数据库查询商品属性信息
     * @param goodsId
     * @return
     */
    @RequestMapping(value = "/queryGoodsAttrListFromDB", method = RequestMethod.POST)
    List<GoodsAttr> queryGoodsAttrListFromDB(@RequestParam("goodsId") Integer goodsId);
    /**
     * 从数据库根据typeId查询属性信息
     * @param typeId
     * @return
     */
    @RequestMapping(value = "/queryGoodsAttributeListByTypeFromDB", method = RequestMethod.POST)
    List<GoodsAttribute> queryGoodsAttributeListByTypeFromDB(@RequestParam("typeId") Short typeId);

    /**
     * 根据type查询所有的属性，然后根据goodsId显示商品已选的属性值
     * @param goodsId
     * @param typeId
     * @return
     */
    @RequestMapping(value = "/queryGoodsAttributeListByGoodsIdAndTypeFromDB", method = RequestMethod.POST)
    List<GoodsAttribute> queryGoodsAttributeListByGoodsIdAndTypeFromDB(@RequestParam("goodsId") Integer goodsId, @RequestParam("typeId") Short typeId);

    /**
     * 从数据库查询商品规格信息
     * @param goodsId
     * @return
     */
    @RequestMapping(value = "/querySpecGoodsPriceListFromDB", method = RequestMethod.POST)
    List<SpecGoodsPrice> querySpecGoodsPriceListFromDB(@RequestParam("goodsId") Integer goodsId);

    /**
     * 从数据库根据typeId查询规格信息
     * @param typeId
     * @return
     */
    @RequestMapping(value = "/querySpecListByTypeFromDB", method = RequestMethod.POST)
    List<Spec> querySpecListByTypeFromDB(@RequestParam("typeId") Short typeId) ;

    /**
     *  从数据库根据typeId查询所有规格信息，然后根据goodsId标上哪些规格项已选中
     * @param goodsId
     * @param typeId
     * @return
     */
    @RequestMapping(value = "/querySpecListByGoodsIdAndTypeIdFromDB", method = RequestMethod.POST)
    List<Spec> querySpecListByGoodsIdAndTypeIdFromDB(@RequestParam("goodsId") Integer goodsId, @RequestParam("typeId") Short typeId);

    /**
     * 保存商品图片到数据库
     * @param imageUrls
     * @param goodsId
     * @return
     */
    @RequestMapping(value = "/saveGoodsImages2DB", method = RequestMethod.POST)
    int saveGoodsImages2DB(@RequestBody String[] imageUrls, @RequestParam("goodsId") Integer goodsId);

    /**
     * 保存商品规格价格和属性到数据库
     * @param specPriceAttrVo
     */
    @RequestMapping(value = "/saveGoodsSpecAndAttr2DB", method = RequestMethod.POST)
    void saveGoodsSpecAndAttr2DB(@RequestBody GoodsSpecPriceAttrVo specPriceAttrVo);

    /**
     * 发布所有商品到mongodb
     */
    @RequestMapping(value = "/publishAll2MongoDB")
    void publishAll2MongoDB();

    /**
     * 发布单个商品到mongodb
     * @param goodsId
     */
    @RequestMapping(value = "/publishGoods2MongoDB", method = RequestMethod.POST)
    void publishGoods2MongoDB(@RequestParam("goodsId") Integer goodsId);

    @RequestMapping(value = "/publishGoods2MongoDB1", method = RequestMethod.POST)
    void publishGoods2MongoDB(List<Goods> goods);

    /**
     * 查询热销商品
     * @param showNum
     * @return
     */
    @RequestMapping(value = "/queryHotSellingGoods", method = RequestMethod.POST)
    List<HotSellingGoods> queryHotSellingGoods(@RequestParam("showNum") Integer showNum);

    @RequestMapping(value = "/updateBySpecGoodsId", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    int updateBySpecGoodsIds(@RequestBody List<SpecGoodsPrice> records);
}
