package cn.enjoy.sys.service;

import cn.enjoy.sys.model.Department;
import cn.enjoy.sys.model.DepartmentTree;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@RequestMapping("/user/sys/service/IDepartmentService")
public interface IDepartmentService {

    @RequestMapping(value = "/saveDepartment", method = RequestMethod.POST)
    Department saveDepartment(@RequestBody Department dept);

    @RequestMapping(value = "/editDepartment", method = RequestMethod.POST)
    void editDepartment(@RequestBody Department dept);

    @RequestMapping(value = "/loadDepartment", method = RequestMethod.POST)
    List<Department> loadDepartment(@RequestBody Department dept);

    @RequestMapping(value = "/removeDepartment", method = RequestMethod.POST)
    void removeDepartment(@RequestParam("id") String id);

    @RequestMapping(value = "/selectDepartmentById", method = RequestMethod.POST)
    Department selectDepartmentById(@RequestParam("id") String id);

    @RequestMapping(value = "/selectByToTree", method = RequestMethod.POST)
    List<DepartmentTree> selectByToTree(@RequestParam("deptName") String deptName, @RequestParam("parentId") String parentId);

    @RequestMapping(value = "/selectByUserId", method = RequestMethod.POST)
    List<Department> selectByUserId(@RequestParam("userId") String userId);

}
