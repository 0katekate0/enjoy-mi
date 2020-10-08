package cn.enjoy.mall.service.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.model.KillGoodsPrice;
import cn.enjoy.mall.vo.KillGoodsSpecPriceDetailVo;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping("/product/mall/service/manage/IKillSpecManageService")
public interface IKillSpecManageService {

    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    int delete(@RequestParam("id") Integer id);

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    int save(@RequestBody KillGoodsPrice record);

    @RequestMapping(value = "/selectCountBySpecGoodId", method = RequestMethod.POST)
    int selectCountBySpecGoodId(@RequestParam("specGoodsId") Integer specGoodsId);

    @RequestMapping(value = "/selectByPrimaryKey", method = RequestMethod.POST)
    KillGoodsPrice selectByPrimaryKey(@RequestParam("id") Integer id);

    @RequestMapping(value = "/detailBySpecGoodId", method = RequestMethod.POST)
    KillGoodsSpecPriceDetailVo detailBySpecGoodId(@RequestParam("id") Integer id);

    @RequestMapping(value = "/detailById", method = RequestMethod.POST)
    KillGoodsSpecPriceDetailVo detailById(@RequestParam("id") Integer id);

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    int update(@RequestBody KillGoodsPrice record);

    @RequestMapping(value = "/updateSecKill", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    int updateSecKill(@RequestBody KillGoodsPrice record);

    @RequestMapping(value = "/updateBySpecGoodsId", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    int updateBySpecGoodsId(@RequestBody KillGoodsPrice record);

    @RequestMapping(value = "/flushCache", method = RequestMethod.POST)
    void flushCache(@RequestBody KillGoodsPrice record);

    @RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    GridModel<KillGoodsSpecPriceDetailVo> queryByPage(@RequestParam("name") String name, @RequestParam("page") int page,
                                                      @RequestParam("pageSize") int pageSize);

    @RequestMapping(value = "/queryView", method = RequestMethod.POST)
    GridModel<KillGoodsSpecPriceDetailVo> queryView(@RequestParam("page") int page, @RequestParam("pageSize") int pageSize);

}
