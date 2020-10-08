package cn.enjoy.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.model.GoodsCategory;
import cn.enjoy.mall.service.IGoodsCategoryService;
import cn.enjoy.mall.service.manage.ICategoryManageService;
import cn.enjoy.sys.controller.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 商品目录管理
 * @author Jack
 * @date 2018/3/12.
 */
@RestController
@RequestMapping("/api/goodscategory")
public class GoodsCategoryController extends BaseController {

    @Autowired
    private ICategoryManageService categoryManageService;
    @Autowired
    private IGoodsCategoryService goodsCategoryService;

    /**
     * 商品目录查询
    * @param page
     * @param rows
     * @param parentId
     * @param categoryName
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("/queryByPage")
    public HttpResponseBody queryByPage(int page, int rows, String parentId, String categoryName){
        return HttpResponseBody.successResponse("ok", categoryManageService.queryByPage(page, rows, parentId, categoryName));
    }

    /**
     * 获取商品2级目录树
    * @param parentId
     * param keywords
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("/getCategoryTree")
    public HttpResponseBody getCategoryTree(String parentId, String keywords){
        return HttpResponseBody.successResponse("ok", goodsCategoryService.selectCategoryTree(parentId, keywords));
    }

    /**
     * 获取商品3级目录树
    * @param parentId
     * @param keywords
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("/getCategoryTree3")
    public HttpResponseBody getCategoryTree3(String parentId, String keywords){
//        List<CategoryTree> categoryTrees = goodsCategoryService.selectCategoryTree3(parentId, keywords);
        return HttpResponseBody.successResponse("ok", goodsCategoryService.selectCategoryTree3(parentId, keywords));
    }

    /**
     * 商品目录保存
    * @param goodsCategory
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/save")
    public HttpResponseBody save(GoodsCategory goodsCategory){
        categoryManageService.save(goodsCategory);
        return HttpResponseBody.successResponse("保存成功");
    }

    /**
     * 商品目录删除
    * @param id
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/delete")
    public HttpResponseBody delete(Short id){
        categoryManageService.delete(id);
        return HttpResponseBody.successResponse("删除成功");
    }

    /**
     * 商品目录批量删除
    * @param ids 目录id数组
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/batchDelete")
    public HttpResponseBody batchDelete(String[] ids){
        if(ids == null || ids.length == 0){
            return HttpResponseBody.failResponse("请选择要删除的分类");
        }
        categoryManageService.deleteByIds(ids);
        return HttpResponseBody.successResponse("批量删除成功");
    }

    /**
     * 刷新目录缓存
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("/refresh")
    public HttpResponseBody refresh(){
        goodsCategoryService.produceHotCategories();
        goodsCategoryService.produceCategory4Home();
        goodsCategoryService.produceCategoryGoodsCount();
        return HttpResponseBody.successResponse("刷新缓存成功");
    }
}
