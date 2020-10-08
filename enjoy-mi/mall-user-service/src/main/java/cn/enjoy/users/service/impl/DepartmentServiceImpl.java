package cn.enjoy.users.service.impl;

import cn.enjoy.core.exception.BusinessException;
import cn.enjoy.core.utils.CommonConstant;
import cn.enjoy.core.utils.UUIDGenerator;
import cn.enjoy.sys.model.Department;
import cn.enjoy.sys.model.DepartmentTree;
import cn.enjoy.sys.model.UserDepartmentInfo;
import cn.enjoy.sys.service.IDepartmentService;
import cn.enjoy.users.dao.DepartmentMapper;
import cn.enjoy.users.dao.UserDepartmentInfoMapper;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * 部门管理
 *
 * @author Jack
 * @date 2020/9/8
 */
@RestController
//@RequestMapping("/user/sys/service/IDepartmentService")
public class DepartmentServiceImpl implements IDepartmentService {

    @Resource
    private DepartmentMapper deptmapper;

    @Resource
    private UserDepartmentInfoMapper userDepartmentInfoMapper;


    /**
     * 保存部门
     *
     * @param dept
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/saveDepartment", method = RequestMethod.POST)
    @Override
    public Department saveDepartment(Department dept) {
        dept.setId(UUIDGenerator.getUUID());
        dept.setStatus(String.valueOf(CommonConstant.VALID));
        if (deptmapper.isExistSameName(dept) > 0) {
            throw new BusinessException("存在同名兄弟部门！");
        }

        deptmapper.insert(dept);
        return dept;
    }

    /**
     * 修改部门
     *
     * @param dept
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/editDepartment", method = RequestMethod.POST)
    @Override
    public void editDepartment(Department dept) {
        deptmapper.updateByPrimaryKeySelective(dept);
    }

    /**
     * 查询部门
     *
     * @param dept
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/loadDepartment", method = RequestMethod.POST)
    @Override
    public List<Department> loadDepartment(Department dept) {
        return deptmapper.loadDepartment(dept);
    }

    /**
     * 删除部门
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/removeDepartment", method = RequestMethod.POST)
    @Override
    public void removeDepartment(String id) throws BusinessException {

        Department dept = deptmapper.selectByPrimaryKey(id);
        List<UserDepartmentInfo> userDepartmentInfos = userDepartmentInfoMapper.selectByDepartmentId(id);
        if (!CollectionUtils.isEmpty(userDepartmentInfos)) {
            throw new BusinessException("请先将该部门下的所有员工删除再进行此操作！");
        }
        dept.setStatus(String.valueOf(CommonConstant.INVALID));
        deptmapper.updateByPrimaryKeySelective(dept);
    }

    /**
     * 根据部门id查询部门
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectDepartmentById", method = RequestMethod.POST)
    @Override
    public Department selectDepartmentById(String id) {
        return deptmapper.selectByPrimaryKey(id);
    }

    /**
     * 查询部门树
     *
     * @param deptName
     * @param parentId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectByToTree", method = RequestMethod.POST)
    @Override
    public List<DepartmentTree> selectByToTree(String deptName, String parentId) {
        List<DepartmentTree> tree = deptmapper.selectToTree(deptName, parentId);
        return tree;
    }

    /**
     * 根据用户id查询部门
     *
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectByUserId", method = RequestMethod.POST)
    @Override
    public List<Department> selectByUserId(String userId) {
        return deptmapper.selectByUserId(userId);
    }

}
