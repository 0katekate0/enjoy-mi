/**
 * Created on 2015年9月2日 by Caiming
 */
package cn.enjoy.sys.service;


import cn.enjoy.core.utils.GridModel;
import cn.enjoy.sys.model.SelectModel;
import cn.enjoy.sys.model.SysDictData;
import cn.enjoy.sys.model.SysDictType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2009</p>
 * <p>Company:享学信息科技有限公司 Co., Ltd.</p>
 * @author Caiming
 * @version 1.0
 * 修改记录：
 * 修改序号，修改日期，修改人，修改内容
 */
@RequestMapping("/user/sys/dictData")
public interface IDictDataService {

	/**
	 * 根据TYPE查询字典数据
	 * <li>创建人：Caiming</li>
	 * <li>创建时间：2015年9月2日</li>
	 * <li>创建目的：【】</li>
	 * <li>修改目的：【修改人：，修改时间：】</li>
	 * @param type
	 * @return
	 */
	@RequestMapping(value = "/findDictByType", method = RequestMethod.POST)
	List<SysDictData> findDictByType(@RequestParam("type") String type);

	/**
	 * 根据TYPE查询字典数据
	 * <li>创建人：huangjianwen</li>
	 * <li>创建时间：2017年10月9日</li>
	 * <li>创建目的：【】</li>
	 * <li>修改目的：【修改人：，修改时间：】</li>
	 * @param code
	 * @return
	 */
	@RequestMapping(value = "/queryDictDataByCode", method = RequestMethod.POST)
	List<SysDictData> queryDictDataByCode(@RequestParam("code") String code);



	
	/**
	 * 查询所有字典数据
	 * <li>创建人：Caiming</li>
	 * <li>创建时间：2015年9月2日</li>
	 * <li>创建目的：【】</li>
	 * <li>修改目的：【修改人：，修改时间：】</li>
	 * @return
	 */
	@RequestMapping(value = "/findAllDict")
	List<SysDictData> findAllDict();

	@RequestMapping(value = "/findAllDictType")
	List<SysDictType> findAllDictType();

	/**
	 * 根据字典type查询字典下拉框
	 * <li>创建人：Caiming</li>
	 * <li>创建时间：2015年9月9日</li>
	 * <li>创建目的：【】</li>
	 * <li>修改目的：【修改人：，修改时间：】</li>
	 * @param type
	 * @return
	 */
	@RequestMapping(value = "/findSelectModelsByType", method = RequestMethod.POST)
	List<SelectModel> findSelectModelsByType(@RequestParam("type") String type);

	/**
	 * 根据字典type查询 并且 RESV1 升序排序
	 * <li>创建人：maojia</li>
	 * <li>创建时间：2015年9月9日</li>
	 * <li>创建目的：【】</li>
	 * <li>修改目的：【修改人：，修改时间：】</li>
	 * @param
	 *     param
	 *     必须参数:
	 *            type : 字典类型
	 * @return
	 */
	@RequestMapping(value = "/queryDictDataByTypeAndORDER", method = RequestMethod.POST)
	List<SysDictData> queryDictDataByTypeAndORDER(@RequestBody Map param);


	@RequestMapping(value = "/queryAllDictDataPage", method = RequestMethod.POST)
	GridModel<SysDictData> queryAllDictDataPage(@RequestParam("param") String param, @RequestParam("type") String type,
												@RequestParam("pageNo") Integer pageNo, @RequestParam("pageSize") Integer pageSize,
												@RequestParam("sidx") String sidx, @RequestParam("sord") String sord);

	@RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
	void deleteByIds(@RequestBody List<String> ids);

	@RequestMapping(value = "/addDictData", method = RequestMethod.POST)
	void addDictData(@RequestBody SysDictData sysDictData);

	@RequestMapping(value = "/selectByPrimaryKey", method = RequestMethod.POST)
	SysDictData selectByPrimaryKey(@RequestParam("id") String id);

	@RequestMapping(value = "/deleteDictData", method = RequestMethod.POST)
	void deleteDictData(@RequestParam("id") String id);

	@RequestMapping(value = "/freezeDictData", method = RequestMethod.POST)
	void freezeDictData(@RequestParam("id") String id, @RequestParam("status") String status);

	@RequestMapping(value = "/updateDictData", method = RequestMethod.POST)
	void updateDictData(@RequestBody SysDictData sysDictData);
}

