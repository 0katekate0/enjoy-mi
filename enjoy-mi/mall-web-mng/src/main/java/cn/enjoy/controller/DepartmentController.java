package cn.enjoy.controller;


import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.sys.controller.BaseController;
import cn.enjoy.sys.model.Department;
import cn.enjoy.sys.model.DepartmentTree;
import cn.enjoy.sys.model.SelectModel;
import cn.enjoy.sys.service.IDepartmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;


/**
 * 部门管理
 * @author Jack
 */
@RestController
@RequestMapping("/api/department/")
public class DepartmentController extends BaseController {

    @Autowired
    private IDepartmentService iDepartmentService;

    /**
     * 新增部门
    * @param dept
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return 
    * @version
    */ 
    @PostMapping("addDepartment")
    public HttpResponseBody addDepartment(Department dept) {
        iDepartmentService.saveDepartment(dept);
        return HttpResponseBody.successResponse("新增成功");
    }

    /**
     * 新增部门修改部门
    * @param deptName 部门名称
    * @param parentId 父id
    * @param deptId 部门id
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("saveDept")
    public HttpResponseBody saveDept(String deptName, String parentId, String deptId) {
        Department department = new Department();
        department.setDepartmentName(deptName);
        department.setId(deptId);
        department.setParentDepartment(parentId);

        if (parentId.equals(deptId)) {
            iDepartmentService.saveDepartment(department);
        } else {
            iDepartmentService.editDepartment(department);
        }
        return HttpResponseBody.successResponse("保存成功");
    }

    /**
     * 删除部门
    * @param id
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("deleteDepartment")
    public HttpResponseBody delete(String id) {
        iDepartmentService.removeDepartment(id);
        return HttpResponseBody.successResponse("删除成功");
    }

    /**
     * 修改部门
    * @param department
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("updateDepartment")
    public HttpResponseBody update(Department department) {
        iDepartmentService.editDepartment(department);
        return HttpResponseBody.successResponse("修改成功");
    }

    /**
     * 查询部门树
    * @param deptName 部门名称
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("queryDepartmentTree")
    public HttpResponseBody<List<DepartmentTree>> queryToTree(@RequestParam(required = false, defaultValue = "") String deptName) {
        List<DepartmentTree> departments = iDepartmentService.selectByToTree(deptName, "");
        return HttpResponseBody.successResponse("查询成功", departments);
    }

    /**
     * 部门信息下拉框
    * @author Jack
    * @date 2020/9/7
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("getDepartmentOptions")
    public HttpResponseBody<List<SelectModel>> queryDepartment() {
        List<Department> departments = iDepartmentService.loadDepartment(new Department());
        List<SelectModel> result = new ArrayList<>();
        for (Department depart : departments) {
            SelectModel sm = new SelectModel();
            sm.setKey(depart.getId());
            sm.setValue(depart.getDepartmentName());
            result.add(sm);
        }
        return HttpResponseBody.successResponse("查询成功", result);
    }


}
