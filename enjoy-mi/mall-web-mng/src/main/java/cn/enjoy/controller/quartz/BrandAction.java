package cn.enjoy.controller.quartz;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.service.manage.IBrandService;
import cn.enjoy.sys.controller.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 品牌信息管理
 *
 * @author Jack
 * @date 2020/9/7
 */
@RestController
@RequestMapping("/api/brand")
public class BrandAction extends BaseController {
    @Autowired
    private IBrandService brandService;

    /**
     * 查询品牌
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/7
     * @version
     */
    @GetMapping("/getAll")
    public HttpResponseBody getAll() {
        return HttpResponseBody.successResponse("ok", brandService.getAll());
    }

}
