package cn.enjoy.sys.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.sys.model.Department;
import cn.enjoy.sys.model.MenuModel;
import cn.enjoy.sys.model.SysUser;
import cn.enjoy.sys.service.IDepartmentService;
import cn.enjoy.sys.service.ILoginService;
import cn.enjoy.sys.service.IUserService;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PersonalController
 */
@RestController
@RequestMapping("/api/personal/")
public class PersonalController extends BaseController {
    @Autowired
    private IUserService iUserService;
    @Autowired
    private IDepartmentService iDepartmentService;
    @Autowired
    private ILoginService iLoginService;


    @GetMapping("getPersonalInfo")
    public HttpResponseBody<Map<String, Object>> getPersonInfo() {
        SysUser sysUser = iUserService.selectByPrimaryKey(getSessionUserId());
        if(sysUser == null){
            return HttpResponseBody.failResponse("请重新登录");
        }
        List<Department> departments = iDepartmentService.selectByUserId(getSessionUserId());
        sysUser.setPassword(null);
        sysUser.setPasswordRand(null);
        Map<String, Object> result = new HashMap<>();
        List<MenuModel> menuModels = iLoginService.queryPermissionList(JSONObject.toJSONString(sysUser));
        result.put("userInfo", sysUser);
        result.put("departments", departments);
        result.put("authorityInfo", menuModels);
        return HttpResponseBody.successResponse("查询成功", result);
    }







    @PostMapping("updatePersonalInfo")
    public HttpResponseBody updatePersonalInfo(SysUser user, @RequestParam(name = "departIds", required = false, defaultValue = "") String[] departIds,
                                               @RequestParam(name = "roleIds", required = false, defaultValue = "") String[] roleIds) {
        iUserService.update(user, JSONObject.toJSONString(departIds), JSONObject.toJSONString(roleIds));
        return HttpResponseBody.successResponse("修改成功");
    }

    @PostMapping("updateUserInfo")
    public HttpResponseBody updateUserInfo(SysUser user) {
        user.setId(getSessionUserId());
        iUserService.updateUserInfo(user);
        return HttpResponseBody.successResponse("修改成功");
    }






}
