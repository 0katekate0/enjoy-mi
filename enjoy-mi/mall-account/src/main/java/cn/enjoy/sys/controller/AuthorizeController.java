package cn.enjoy.sys.controller;

import cn.enjoy.core.exception.BusinessException;
import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.core.utils.response.ResponseCodeConstant;
import cn.enjoy.mall.constant.SsoConstants;
import cn.enjoy.sys.service.IAuthorizeService;
import cn.enjoy.util.ShiroCacheUtil;
import org.apache.oltu.oauth2.as.issuer.MD5Generator;
import org.apache.oltu.oauth2.as.issuer.OAuthIssuerImpl;
import org.apache.oltu.oauth2.as.request.OAuthAuthzRequest;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.ResponseType;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * 鉴权管理
 *
 * @author Jack
 * @date 2018/3/23.
 */
@RestController
@RequestMapping("/api")
public class AuthorizeController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private IAuthorizeService authorizeService;

    @Resource
    private ShiroCacheUtil shiroCacheUtil;

    /**
     * 校验是否登录及后去授权码
     *
     * @param model
     * @param request
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/9
     * @version
     */
    @RequestMapping("authorize")
    public Object authorize(Model model, HttpServletRequest request) throws OAuthSystemException, URISyntaxException {

        //构建OAuth请求
        OAuthAuthzRequest oAuthAuthzRequest = null;
        try {
            oAuthAuthzRequest = new OAuthAuthzRequest(request);

            // 根据传入的clientId 判断 客户端是否存在
            if (!authorizeService.checkClientId(oAuthAuthzRequest.getClientId())) {
                return HttpResponseBody.failResponse("客户端验证失败，如错误的client_id/client_secret");
            }

            // 判断用户是否登录
            Subject subject = SecurityUtils.getSubject();
            logger.info("------------------subject.hashCode():--------------" + subject.hashCode());
            logger.info("------------------subject.isAuthenticated():--------------" + subject.isAuthenticated());
            logger.info("------------------subject.getPrincipal():--------------" + subject.getPrincipal());

//            SysUser user = (SysUser) subject.getSession().getAttribute(CommonConstant.SESSION_USER_KEY);
//            logger.info("------------------user:--------------" + user);
            if (!subject.isAuthenticated()) {
                if (!login(subject, request)) {
                    logger.info("------------------not login:--------------");
                    return new HttpResponseBody(ResponseCodeConstant.UN_LOGIN_ERROR, "没有登陆");
                }
            }
//            if (user == null) {
//                if (!login(subject, request)) {
//                    logger.info("------------------not login:--------------");
//                    return new HttpResponseBody(ResponseCodeConstant.UN_LOGIN_ERROR, "没有登陆");
//                }
//            }
//            logger.info("------------------user:--------------" + user.getUserName());
            String username = (String) subject.getPrincipal();

            //生成授权码
            String authorizationCode = null;

            String responseType = oAuthAuthzRequest.getParam(OAuth.OAUTH_RESPONSE_TYPE);
            logger.info("------------------responseType:" + responseType);
            if (responseType.equals(ResponseType.CODE.toString())) {
                OAuthIssuerImpl oAuthIssuer = new OAuthIssuerImpl(new MD5Generator());
                authorizationCode = oAuthIssuer.authorizationCode();
                logger.info("------------------authorizationCode:--------------" + authorizationCode);
                shiroCacheUtil.addAuthCode(authorizationCode, username);
                logger.info("------------------put cache ok--------------");
            }

            Map<String, Object> data = new HashMap<>();
            data.put(SsoConstants.AUTH_CODE, authorizationCode);
            return HttpResponseBody.successResponse("ok", data);

        } catch (OAuthProblemException e) {
            return HttpResponseBody.failResponse(e.getMessage());
        }
    }

    /**
     * 用户登录接口
     *
     * @param subject
     * @param request
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/9
     * @version
     */
    private boolean login(Subject subject, HttpServletRequest request) {
        logger.info("-----------------request.getMethod():" + request.getMethod());
        if ("get".equalsIgnoreCase(request.getMethod())) {
            return false;
        }

        String username = request.getParameter("username");
        String password = request.getParameter("password");
        logger.info("------------------username:password--------------" + username + ":" + password);
        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
            return false;
        }

        UsernamePasswordToken token = new UsernamePasswordToken(username, password);

        try {
            subject.login(token);
            return true;
        } catch (Exception e) {
            throw new BusinessException("登录失败");
            //request.setAttribute("error","登录失败"+e.getClass().getName());
            //return false;
        }
    }
}
