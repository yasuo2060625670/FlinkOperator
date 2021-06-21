import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import com.typesafe.config.ConfigMergeable;
/**
 * @author ：zz
 */
public class testtt {
    public static void main(String[] args) throws Exception {
        Thread thread = new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                run2();
            }
        });
        thread.start();

    }

    public  static void run2() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldTypes = new TypeInformation[]{
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/icbc?serverTimezone=UTC&amp")
                .setUsername("itdw")
                .setPassword("itdw#123")
                .setQuery("select * from a;")
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Row> input = env.createInput(jdbcInputFormat);
        input.print();

        jdbcInputFormat.closeInputFormat();
    }
}
