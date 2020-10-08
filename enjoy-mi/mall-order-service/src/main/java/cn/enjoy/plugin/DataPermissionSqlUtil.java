package cn.enjoy.plugin;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.io.StringReader;
import java.util.List;

/**
 * @Classname DataPermissionSqlUtil
 * @Description TODO
 * @Author Jack
 * Date 2020/8/5 16:57
 * Version 1.0
 */
public class DataPermissionSqlUtil {
    private static CCJSqlParserManager pm = new CCJSqlParserManager();


    /**
     * detect table names from given table
     * ATTENTION : WE WILL SKIP SCALAR SUBQUERY IN PROJECTION CLAUSE
     */
    public static List<String> getTableNames(String sql) throws Exception {
        List<String> tablenames = null;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        Statement statement = pm.parse(new StringReader(sql));
        if (statement instanceof Select) {
            tablenames = tablesNamesFinder.getTableList((Select) statement);
        } else if (statement instanceof Update) {
            tablenames = tablesNamesFinder.getTableList((Update) statement);
        } else if (statement instanceof Delete) {
            tablenames = tablesNamesFinder.getTableList((Delete) statement);
        } else if (statement instanceof Replace) {
            tablenames = tablesNamesFinder.getTableList((Replace) statement);
        } else if (statement instanceof Insert) {
            tablenames = tablesNamesFinder.getTableList((Insert) statement);
        }
        return tablenames;
    }

    public static void main(String[] args) {
        String sql = "insert into tp_order(orderId,orderName) values (?,?)";
        try {
            List<String> tablenames = getTableNames(sql);
            System.out.println(tablenames);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
