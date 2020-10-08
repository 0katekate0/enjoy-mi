package cn.enjoy.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.model.KillGoodsPrice;
import cn.enjoy.mall.service.manage.IKillSpecManageService;
import cn.enjoy.sys.controller.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 商品秒杀
* @author Jack
* @date 2020/9/7
*/
@RestController
@RequestMapping("/api/killgoodsSpec")
public class KillSpecController extends BaseController {

    @Autowired
    private IKillSpecManageService iKillSpecManageService;

    /**
     * 秒杀商品查询
    * @param name
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("/queryByPage")
    public HttpResponseBody queryByPage(String name){
        return HttpResponseBody.successResponse("ok", iKillSpecManageService.queryByPage(name,1,20));
    }

    /**
     * 秒杀商品详细信息查询
    * @param id
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("/detail")
    public HttpResponseBody detail(Integer id) {
        return HttpResponseBody.successResponse("ok", iKillSpecManageService.selectByPrimaryKey(id));
    }

    /**
     * 秒杀商品保存
    * @param killGoodsPrice
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/save")
    public HttpResponseBody save(KillGoodsPrice killGoodsPrice){
        if (killGoodsPrice.getId() == null || killGoodsPrice.getId() == 0){
            if (iKillSpecManageService.selectCountBySpecGoodId(killGoodsPrice.getSpecGoodsId()) > 0){
                return HttpResponseBody.failResponse("同一商品规格不能重复加入秒杀");
            }
            iKillSpecManageService.save(killGoodsPrice);
        } else {
            KillGoodsPrice killGoods = iKillSpecManageService.selectByPrimaryKey(killGoodsPrice.getId());
            if (killGoods.getStatus() ==1 && killGoods.getBegainTime().getTime() < System.currentTimeMillis()){
                iKillSpecManageService.flushCache(killGoods);
                return HttpResponseBody.failResponse("秒杀已运行，不支持修改");
            }

            iKillSpecManageService.update(killGoodsPrice);
        }

        return HttpResponseBody.successResponse("保存成功");
    }

    /**
     * 秒杀商品删除
    * @param id
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/delete")
    public HttpResponseBody delete(Integer id){
        iKillSpecManageService.delete(id);
        return HttpResponseBody.successResponse("删除成功");
    }



}
