package cn.enjoy.plugin;

import cn.enjoy.mall.model.Order;
import cn.enjoy.mall.model.OrderAction;
import cn.enjoy.mall.model.OrderGoods;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.plugin.*;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @Classname RouteTablePlugin
 * @Description 用于做普通订单和秒杀订单的路由逻辑
 * @Author Jack
 * Date 2020/8/5 15:57
 * Version 1.0
 */
//@Component
@Slf4j
@Intercepts({@Signature(type = StatementHandler.class, method = "prepare",
        args = {Connection.class, Integer.class})})
public class RouteTablePlugin implements Interceptor {
    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        if (invocation.getTarget() instanceof StatementHandler) {
            StatementHandler statementHandler = (StatementHandler) invocation.getTarget();

            //如果不需要拦截，则直接调用被代理方法
            if (!isNeedRoute(statementHandler.getBoundSql().getParameterObject())) {
                return invocation.proceed();
            }

            statementHandler.getParameterHandler().getParameterObject();
            Field delegate = getField(statementHandler, "delegate");
            StatementHandler prepareStatement = (StatementHandler) delegate.get(statementHandler);

            Field boundSql = getField(prepareStatement, "boundSql");
            BoundSql bsinstance = (BoundSql) boundSql.get(prepareStatement);

            Field sql = getField(bsinstance, "sql");
            String sqlStr = (String) sql.get(bsinstance);

            Object object = bsinstance.getParameterObject();

            if (isKillOrder(object)) {
                sqlStr = routeSql(sqlStr, object);
            }
//            log.info("----------RouteTablePlugin--sql-->" + sqlStr);
            sql.set(bsinstance, sqlStr);
        }
        return invocation.proceed();
    }

    private String routeSql(String sqlStr, Object obj) {
        try {
            List<String> tableNames = DataPermissionSqlUtil.getTableNames(sqlStr);
            if (tableNames != null && tableNames.size() == 1) {
                log.info("------------RouteTablePlugin--tableName-->" + tableNames.get(0) + "_kill");
                return sqlStr.replaceFirst(tableNames.get(0), tableNames.get(0) + "_kill");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sqlStr;
    }

    private Boolean isNeedRoute(Object obj) {
        if (obj instanceof MapperMethod.ParamMap) {
            MapperMethod.ParamMap paramMap = (MapperMethod.ParamMap) obj;
            Object list = paramMap.get("list");
            if (list != null && (((List) list).get(0) instanceof OrderGoods)) {
                return true;
            }
        }

        //针对查询
        if (obj instanceof Map) {
            Map map = (Map) obj;
            Object orderType = ((Map) obj).get("orderType");
            if (orderType != null && !"".equals(orderType.toString()) && "K".equalsIgnoreCase(orderType.toString())) {
                return true;
            }
        }
        return obj instanceof Order || obj instanceof OrderAction;
    }

    private Boolean isKillOrder(Object obj) {
        if (obj instanceof Order) {
            return "K".equalsIgnoreCase(((Order) obj).getOrderType());
        } else if (obj instanceof MapperMethod.ParamMap) {
            MapperMethod.ParamMap paramMap = (MapperMethod.ParamMap) obj;
            Object list = paramMap.get("list");
            if (list != null) {
                Object o = ((List) list).get(0);
                if (o != null) {
                    return "K".equalsIgnoreCase(((OrderGoods) o).getOrderType());
                } else {
                    return false;
                }
            }
            return false;
        } else if (obj instanceof OrderAction) {
            return "K".equalsIgnoreCase(((OrderAction) obj).getOrderType());
        } else if (obj instanceof Map) {
            Map map = (Map) obj;
            Object orderType = ((Map) obj).get("orderType");
            if (orderType != null && !"".equals(orderType.toString()) && "K".equalsIgnoreCase(orderType.toString())) {
                return true;
            }
        }
        return false;
    }

    private Field getField(Object o, String name) {
        Field field = ReflectionUtils.findField(o.getClass(), name);
        ReflectionUtils.makeAccessible(field);
        return field;
    }

    @Override
    public Object plugin(Object target) {
        if (target instanceof RoutingStatementHandler) {
            return Plugin.wrap(target, this);
        }
        return target;
    }
}
