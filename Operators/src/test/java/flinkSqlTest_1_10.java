import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;


/**
 * @author ：zz
 */

public class flinkSqlTest_1_10 {

    /**1.创建环境->拿到env操作环境对象
     * 2.拿到表tEnv操作环境对象
     *
     *
     * env.addSource();->flink得数据源
     *
     *
     * @throws Exception
     */
    @Test
    public void  test() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

/**
 * 连接kafka
 */
        tEnv.sqlUpdate("CREATE TABLE test (\n" +
//                "`offset` BIGINT metadata ,"+ //目前版本貌似不支持
                "  name STRING,\n" +
                "  age INT,\n" +
                "  gender STRING\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'final_data',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "\n" +
                "  'update-mode' = 'append',\n" +
                "\n" +
                "  'format.type' = 'avro',\n" +
                "  'format.avro-schema' = '{\n" +
                "    \"type\": \"record\",\n" +
                "    \"name\": \"paas_log_tmp_5\",\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"name\": \"name\",\n" +
                "            \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"age\",\n" +
                "            \"type\": \"int\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"gender\",\n" +
                "            \"type\": \"string\"\n" +
                "        }\n" +
                "    ]\n" +
                "}'\n" +
                ")");

        Table table = tEnv.sqlQuery(
                "SELECT * from test");


        /*
        连接JDBC
         */
        tEnv.sqlUpdate("CREATE TABLE zwtest (\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  gender string\n" +
                ") WITH (\n" +
                "  'connector.type' = 'jdbc', \n" +


                /*
                * mysql
                * */

                "  'connector.password' = 'test',\n" + "  'connector.url' = 'jdbc:mysql://10.7.213.158:3306/test?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC&autoReconnect=true&failOverReadOnly=false&useSSL=false', \n" +
                "  'connector.table' = 'zwtest0107',  \n" +//数据库表
                "  'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "  'connector.username' = 'test', \n" +
                "  'connector.password' = 'test',\n" +
                //写
                "  'connector.write.flush.max-rows' = '5000', \n" +
                "  'connector.write.flush.interval' = '2s',\n" +
                "  'connector.write.max-retries' = '3'\n" +
                ")");
        tEnv.sqlUpdate("INSERT INTO zwtest\n" +
                "SELECT  name, age, gender FROM test");//flink表

        DataStream<Row> ds = tEnv.toAppendStream(table, Row.class);
//        ds.print();
        env.execute();
    }
}




