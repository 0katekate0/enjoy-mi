package cn.enjoy.sys.service;

import cn.enjoy.sys.model.Oauth2Client;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

/**
 * @author Ray
 * @date 2018/3/23.
 */
@RequestMapping("/user/sys/service/IAuthorizeService")
public interface IAuthorizeService {

     /**
      * 验证oauth2.0客户端是否存在
      *
      * @param clientId
      * @return
      * @throws Exception
      * @author Jack
      * @date 2020/9/8
      * @version
      */
     @RequestMapping(value = "/checkClientId", method = RequestMethod.POST)
     boolean checkClientId(@RequestParam("clientId") String clientId) ;

     /**
      * 验证oauth2.0密码是否存在
      *
      * @param clientSecret
      * @return
      * @throws Exception
      * @author Jack
      * @date 2020/9/8
      * @version
      */
     @RequestMapping(value = "/checkClientSecret", method = RequestMethod.POST)
     boolean checkClientSecret(@RequestParam("clientSecret") String clientSecret) ;
     /**
      * 获取过期时间
      *
      * @return
      * @throws Exception
      * @author Jack
      * @date 2020/9/8
      * @version
      */
     @RequestMapping(value = "/getExpireIn")
     long getExpireIn() ;
     /**
      * 根据客户端id获取鉴权客户端信息
      *
      * @param clientId
      * @return
      * @throws Exception
      * @author Jack
      * @date 2020/9/8
      * @version
      */
     @RequestMapping(value = "/findClientByClientId", method = RequestMethod.POST)
     Oauth2Client findClientByClientId(@RequestParam("clientId") String clientId);

     /**
      * 获取所有的鉴权客户端信息
      *
      * @return
      * @throws Exception
      * @author Jack
      * @date 2020/9/8
      * @version
      */
     @RequestMapping(value = "/getClientMap")
     Map<String, Oauth2Client> getClientMap();
}
