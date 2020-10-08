package cn.enjoy.users.aop;

import cn.enjoy.users.dbutils.DBContextHolder;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

//@Aspect
//@Component
//@Order(-1)
public class DataSourceAop {
    /*从库的切点,没有标注Master注解，并且方法名为select和get开头的方法，走从库*/
    @Pointcut("!@annotation(cn.enjoy.users.annotation.Master) " +
            "&& (execution(* cn.enjoy.mall.service.impl..*.select*(..)) " +
            "|| execution(* cn.enjoy.mall.service.impl..*.get*(..))" +
            "|| execution(* cn.enjoy.mall.service.impl..*.find*(..))" +
            "|| execution(* cn.enjoy.mall.service.impl..*.query*(..)))")
    public void slavePointcut() {

    }

    /*主库的切点,或者标注了Master注解或者方法名为insert、update等开头的方法，走主库*/
    @Pointcut("@annotation(cn.enjoy.users.annotation.Master) " +
            "|| execution(* cn.enjoy.mall.service.impl..*.insert*(..)) " +
            "|| execution(* cn.enjoy.mall.service.impl..*.add*(..)) " +
            "|| execution(* cn.enjoy.mall.service.impl..*.update*(..)) " +
            "|| execution(* cn.enjoy.mall.service.impl..*.edit*(..)) " +
            "|| execution(* cn.enjoy.mall.service.impl..*.delete*(..)) " +
            "|| execution(* cn.enjoy.mall.service.impl..*.remove*(..))")
    public void masterPointcut() {
    }

    @Before("slavePointcut()")
    public void slave() {
        DBContextHolder.slave();
    }

    @Before("masterPointcut()")
    public void master() {
        DBContextHolder.master();
    }

    @After("slavePointcut()")
    public void completeSlave(){
        DBContextHolder.remove();
    }

    @After("masterPointcut()")
    public void completeMaster(){
        DBContextHolder.remove();
    }
}
