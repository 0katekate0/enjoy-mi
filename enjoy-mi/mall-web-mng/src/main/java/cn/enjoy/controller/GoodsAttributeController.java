package cn.enjoy.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.model.GoodsAttribute;
import cn.enjoy.mall.service.manage.IAttributeManageService;
import cn.enjoy.sys.controller.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * 商品可选属性
 * @author Jack
 * @date 2018/3/15.
 */
@RestController
@RequestMapping("/api/goodsAttr")
public class GoodsAttributeController extends BaseController {


    @Autowired
    private IAttributeManageService attributeManageService;

    /**
     * 查询属性列表
    * @param page
     * @param rows
     * @param attribute
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("/queryByPage")
    public HttpResponseBody queryByPage(int page, int rows, GoodsAttribute attribute){
        return HttpResponseBody.successResponse("ok", attributeManageService.queryByPage(page, rows, attribute));
    }

    /**
     * 属性保存
    * @param attribute
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/save")
    public HttpResponseBody save(@RequestBody GoodsAttribute attribute){
        attributeManageService.save(attribute);
        return HttpResponseBody.successResponse("保存成功");
    }

    /**
     * 属性更新
     * @param attribute
     * @author Jack
     * @date 2020/9/7
     * @throws Exception
     * @return
     * @version
     */
    @PostMapping("/update")
    public HttpResponseBody update(@RequestBody GoodsAttribute attribute){
        attributeManageService.update(attribute);
        return HttpResponseBody.successResponse("保存成功");
    }


    /**
     * 属性删除
    * @param id
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/delete")
    public HttpResponseBody delete(Short id){
        attributeManageService.delete(id);
        return HttpResponseBody.successResponse("删除成功");
    }

    /**
     * 属性匹配删除
    * @param ids id数组
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/batchDelete")
    public HttpResponseBody batchDelete(String[] ids){
        if(ids == null || ids.length == 0){
            return HttpResponseBody.failResponse("请选择要删除的数据");
        }
        attributeManageService.deleteByIds(ids);
        return HttpResponseBody.successResponse("批量删除成功");
    }


}
