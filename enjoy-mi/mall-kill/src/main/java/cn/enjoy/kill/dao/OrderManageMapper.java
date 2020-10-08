package cn.enjoy.kill.dao;

import cn.enjoy.mall.vo.OrderVo;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import org.apache.ibatis.annotations.Param;


/**
 * @author Ray
 * @date 2018/3/19.
 */
public interface OrderManageMapper {

    Long queryByPageCount(OrderVo params);

    PageList<OrderVo> queryByPage(OrderVo params);

    PageList<OrderVo> secondQuerySql(@Param("addTimeMin") Long addTimeMin, @Param("addTimeMax") Long addTimeMax);
}
