package cn.enjoy.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.model.GoodsType;
import cn.enjoy.mall.service.manage.IGoodsTypeManageService;
import cn.enjoy.sys.controller.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 商品类型
 *
 * @author Ray
 * @date 2018/3/12.
 */
@RestController
@RequestMapping("/api/goodsType")
public class GoodsTypeController extends BaseController {


    @Autowired
    private IGoodsTypeManageService goodsTypeManageService;

    /**
     * 商品类型列表
     *
     * @param page
     * @param rows
     * @param parentId
     * @param name
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @GetMapping("/queryByPage")
    public HttpResponseBody queryByPage(int page, int rows, String parentId, String name) {
        return HttpResponseBody.successResponse("ok", goodsTypeManageService.queryByPage(page, rows, parentId, name));
    }

    /**
     * 查询所有商品类型信息
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @GetMapping("/getAll")
    public HttpResponseBody getAll() {
        return HttpResponseBody.successResponse("ok", goodsTypeManageService.queryAll());
    }

    /**
     * 保存商品类型
     * @param goodsType
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @PostMapping("/save")
    public HttpResponseBody save(GoodsType goodsType) {
        goodsTypeManageService.save(goodsType);
        return HttpResponseBody.successResponse("保存成功");
    }

    /**
     * 删除商品类型
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @PostMapping("/delete")
    public HttpResponseBody delete(Short id) {
        goodsTypeManageService.delete(id);
        return HttpResponseBody.successResponse("删除成功");
    }

    /**
     * 批量删除
     * @param ids
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @PostMapping("/batchDelete")
    public HttpResponseBody batchDelete(String[] ids) {
        if (ids == null || ids.length == 0) {
            return HttpResponseBody.failResponse("请选择要删除的数据");
        }
        goodsTypeManageService.deleteByIds(ids);
        return HttpResponseBody.successResponse("批量删除成功");
    }


}
