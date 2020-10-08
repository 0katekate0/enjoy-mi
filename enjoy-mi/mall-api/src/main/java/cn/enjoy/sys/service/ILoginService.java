package cn.enjoy.sys.service;

import cn.enjoy.sys.model.MenuModel;
import cn.enjoy.sys.model.SysUser;
import org.apache.shiro.authc.AuthenticationToken;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

@RequestMapping("/user/sys/service/ILoginService")
public interface ILoginService {
    @RequestMapping(value = "/loginByToken", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    Map<String, Object> login(@RequestBody AuthenticationToken token);

    @RequestMapping(value = "/loginByTokenStr", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    Map<String, Object> loginByStrToken(@RequestParam("token") String token);

    @RequestMapping(value = "/loginByAccount", method = RequestMethod.POST)
    Map<String, Object> login(@RequestParam("loginAccount") String loginAccount, @RequestParam("password") String password);

    @RequestMapping(value = "/queryMenus", method = RequestMethod.POST)
    List<MenuModel> queryMenus(@RequestParam("userId") String userId, @RequestParam("parentId") String parentId);

    @RequestMapping(value = "/queryPermissionList", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    List<MenuModel> queryPermissionList(@RequestBody SysUser user);

    @RequestMapping(value = "/queryPermissionListStr", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    List<MenuModel> queryPermissionList(@RequestParam("userStr") String userStr);

    @RequestMapping(value = "/login", method = RequestMethod.POST)
    SysUser login(@RequestParam("loginAccount") String loginAccount);
}
