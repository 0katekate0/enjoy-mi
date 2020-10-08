package cn.enjoy.mall.dao;

import cn.enjoy.mall.vo.OrderVo;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import org.apache.ibatis.annotations.Param;


/**
 * @author Ray
 * @date 2018/3/19.
 */
public interface OrderManageMapper {

    PageList<OrderVo> queryByPage(OrderVo params);

    Long queryByPageCount(OrderVo params);

    PageList<OrderVo> secondQuerySql(@Param("addTimeMin") Long addTimeMin,@Param("addTimeMax") Long addTimeMax);
}
