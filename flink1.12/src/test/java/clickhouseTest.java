import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * @author ：zz
 */
public class clickhouseTest {

    @Test
    public void test() throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        StreamExecutionEnvironment env = environment.getEnv();
        Table table = environment.getTable();
        StreamTableEnvironment tEnv = environment.gettEnv();
        tEnv.executeSql("CREATE TABLE flink_test (\n" +
                "    name VARCHAR,\n" +
                "    age INT,\n" +
                "   `timestamp` BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'clickhouse://192.168.8.114:8123',\n" +
                "    'username' = 'default',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'default',        /* ClickHouse 数据库名，默认为 default */\n" +
                "    'table-name' = 'flink_test',      /* ClickHouse 数据表名 */\n" +
                "    'sink.batch-size' = '1000',         /* batch 大小 */\n" +
                "    'sink.flush-interval' = '1000',     /* flush 时间间隔 */\n" +
                "    'sink.max-retries' = '3',           /* 最大重试次数 */\n" +
                "    'sink.partition-strategy' = 'hash', /* hash | random | balanced */\n" +
                "    'sink.partition-key' = 'name',      /* hash 策略下的分区键 */\n" +
                "    'sink.ignore-delete' = 'true'       /* 忽略 DELETE 并视 UPDATE 为 INSERT */\n" +
                ")");

        tEnv.executeSql("insert into flink_test select name,age,`timestamp` from test");
        DataStream<Row> outputStream = environment.getOutputStream();

        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
    env.execute("zhuwei");

    }
}
