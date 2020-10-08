package cn.enjoy.sys.controller;

import cn.enjoy.config.Config;
import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.sys.security.MySessionContext;
import org.apache.shiro.session.Session;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 系统管理
 *
 * @author Jack
 * @date 2017/10/16
 */
@RestController
@RequestMapping("/api/sys")
public class SystemController extends BaseController {

    @Resource
    private Config config;

    /**
     * 获取工程配置信息
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @GetMapping("config")
    public HttpResponseBody getConfig() {
        return HttpResponseBody.successResponse("ok", config);
    }

    /**
     * 登出接口
     * @param sid
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("logout")
    public HttpResponseBody logoutBySessionId(String sid) {
        Session session = MySessionContext.getSession(sid);
        if (session != null) {
            session.stop();
        }
        return HttpResponseBody.successResponse("ok");
    }
}
