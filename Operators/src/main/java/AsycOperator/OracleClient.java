package AsycOperator;

import com.google.common.collect.Lists;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.List;

/**
 * @author ：zz
 */
public class OracleClient {
    public final static Logger LOGGER = LoggerFactory.getLogger(OracleClient.class);

    private final static String DRIVER = "oracle.jdbc.driver.OracleDriver";
    private final static String URL = "jdbc:oracle:thin:@127.0.0.1:1521:ORCl";//Oracle的默认数据库名
    private final static String USER = "scott";// 系统默认的用户名
    private final static String PASSWORD = "scott";// 安装时设置的密码
    private static Connection connection;
    private static PreparedStatement ps;

    static {
        try {
            Class.forName(DRIVER).newInstance();// 加载Oracle驱动程序
            connection = DriverManager.getConnection(URL, USER, PASSWORD);// 获取连接
            ps = connection.prepareStatement("select * from students ");

        } catch (InstantiationException e1) {
            LOGGER.info("实例异常");
        } catch (IllegalAccessException e2) {
            LOGGER.info("访问异常");
        } catch (ClassNotFoundException e3) {
            LOGGER.info("MySQL驱动类找不到");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<Row> query(Row row) {
        System.out.println("query exec");
        try {
            Thread.sleep(10);
//            ps.setString(1, row.getField(0).toString());
            ResultSet resultSet = ps.executeQuery();
            List<Row> lists = Lists.newArrayList();
            while (resultSet.next() && !connection.isClosed()) {
                String string1 = resultSet.getString(1);
                String string2 = resultSet.getString(2);
                String string3 = resultSet.getString(3);
                String string4 = resultSet.getString(4);
                lists.add(Row.of(string1, string2, string3, string4));
            }
            return lists;
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }
}
