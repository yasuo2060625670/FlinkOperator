package flinkTableApi;

import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import flinkTableApi.aggfunctions.MaxFunction;
import flinkTableApi.scalarfunctions.HashFunction;
import flinkTableApi.tablefunctions.SplitFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author ：zz
 */
public class FlinkTableApi {
    public static void main(String[] args) throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        StreamExecutionEnvironment env = environment.getEnv();
        //        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

//        environment.getOutputStream().print();
        StreamTableEnvironment tEnv = environment.gettEnv();

        Table table = tEnv.sqlQuery("select * from test");

/*
file sink
            tEnv.sqlUpdate("create table fileTable(" +
                    "name string," +
                    "age int," +
                    "`timestamp` BigInt)with(" +
                    "  'connector.type' = 'filesystem',               " +
                    "  'connector.path' = 'file:///tmp', " +
                    "  'format.type' = 'csv'          " +
                    ")");
            tEnv.sqlUpdate("insert into fileTable select name,age,`timestamp` from test");
            */

/*
 file read

        tEnv.sqlUpdate("create table fileTable(" +
                "name string," +
                "age int," +
                "`timestamp` BigInt)with(" +
                "  'connector.type' = 'filesystem',               " +
                "  'connector.path' = 'file:///tmp/1', " +
                "  'format.type' = 'csv'          " +
                ")");
        Table table1 = tEnv.sqlQuery("select name,age,`timestamp` from fileTable");
        table.printSchema();
        DataStream<Row> dataStream = tEnv.toAppendStream(table1, Row.class);
        dataStream.print("test2");
*/



/*
kafkaRead

        tEnv.sqlUpdate("CREATE TABLE kafkaRead (\n" +
//                "`offset` BIGINT metadata ,"+ //目前版本貌似不支持
                "  name STRING,\n" +
                "  age INT,\n" +
                "  gender STRING\n," +
                "  time_str string," +
                "t as to_timestamp(time_str),"+
"proctime as PROCTIME(),\n"+
                " WATERMARK FOR t AS t - INTERVAL '5' SECOND"+

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
                "        },{" +
                "            \"name\": \"time_str\",\n" +
                "            \"type\": \"string\"\n" +
                "           }\n" +
                "    ]\n" +
                "}'\n" +
                ")");

        Table table1 = tEnv.sqlQuery("select * from kafkaRead");

*/
/*kakfaSink
        tEnv.sqlUpdate("CREATE TABLE kafkasink(\n" +
//                "`offset` BIGINT metadata ,"+ //目前版本貌似不支持
                "  name STRING,\n" +
                "  age INT,\n" +
                "  gender STRING\n," +
                "time_str string" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = '0.10',\n" +
                "  'connector.topic' = 'final_data2',\n" +
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
                "        },{" +
                "            \"name\": \"time_str\",\n" +
                "            \"type\": \"string\"\n" +
                "           }\n" +
                "    ]\n" +
                "}'\n" +
                ")");


        tEnv.sqlUpdate("insert into kafkasink select * from kafkaRead");
        */


/*
jdbcRead



        tEnv.sqlUpdate("CREATE TABLE zwtest0107 (\n" +
                "  name string,\n" +
                "  age DECIMAL(38,18),\n" +
                "  gender string\n" +
//"t as TO_TIMESTAMP(FROM_UNIXTIME(1615287508107,'yyyy-MM-dd HH:mm:ss'))," +
//                "proctime as proctime(),\n"+
//                "  WATERMARK FOR proctime AS proctime- INTERVAL '5' SECOND"+


                ") WITH (\n" +
                "  'connector.type' = 'jdbc', \n" +



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
        Table table1 = tEnv.sqlQuery("\n" +
                //flink表
                "SELECT  * FROM zwtest0107");
*/
/*
es
 */
//        tEnv.sqlUpdate("CREATE TABLE zwtest0407 (\n" +
//                "  name string,\n" +
//                "event_time string,"+
//                "es_time string,"+
//                "  age DECIMAL(38,18),\n" +
//                "  gender string\n" +
//
//
//                ") WITH (\n" +
//                "  'connector.type' = 'elasticsearch', \n" +
//
//
//
//                "  'connector.version' = '6', \n" +
//                "  'connector.hosts' = 'http://localhost:9200',  \n" +//数据库表
//                "  'connector.index' = 'test_ab_2021-04-01',\n" +
//                "  'connector.document-type' = '_doc', \n" +
//                "  'update-mode' = 'append'\n" +
//
//                ")");
//        Table table1 = tEnv.sqlQuery("\n" +
//                //flink表
//                "SELECT  * FROM zwtest0407");

        table.printSchema();
        tEnv.registerFunction("hash", new HashFunction());
        tEnv.registerFunction("split", new SplitFunction());
        tEnv.registerFunction("maxx", new MaxFunction());


//        Table table1 = tEnv.sqlQuery("select hash(name),hash(age) from test");
//        Table table1 = tEnv.sqlQuery("select test.name,test.age,test.`timestamp`,aaa  from test  join  lateral table(split('张无忌,陈真,c'))as splitid(aaa) on aaa=test.name ");
//        Table table1 = tEnv.sqlQuery("select test.name,test.age,test.`timestamp`,aaa  from test left outer join  lateral table(split('张无忌,陈真,c'))as splitid(aaa) on TRUE ");
//        Table table1 = tEnv.sqlQuery(" select name,count(*) from test group by name ");
//        Table table1 = tEnv.sqlQuery(" select maxx(age) from test group by name");//agg function
//        Table table1 = tEnv.sqlQuery(" select TUMBLE_START(proc_time, interval '2' second) as aaa,count(distinct age),name from test GROUP BY TUMBLE(proc_time, interval '2' second),name");//滚动窗口
//        Table table1 = tEnv.sqlQuery(" select HOP_START(proc_time, interval '1' second,interval '2' second) as aaa,count(distinct age),name from test GROUP BY HOP(proc_time,interval '1' second, interval '2' second),name");
//        Table table1 = tEnv.sqlQuery(" select count(distinct age),name from test GROUP BY SESSION(proc_time, interval '2' second),name");
        //over
//        Table table1 = tEnv.sqlQuery("select rowtime,proctime,name,time_str,cn, max(cn) over (partition by name " +
//                "order by rowtime " +//必须用时间，
//                "RANGe  between interval '2' minute preceding  and current row) as max_cn from tmp");
//        Table table1 = tEnv.sqlQuery(" SELECT \n" +
//                "SESSION_START(proc_time, INTERVAL '2' SECOND),\n" +
//                "SESSION_END(proc_time, INTERVAL '2' SECOND),\n" +
//                "\n" +
//                "COUNT(name)\n" +
//                "FROM test\n" +
//                "GROUP BY SESSION(proc_time, INTERVAL '2' SECOND)");
        Table table1 = tEnv.sqlQuery("select * from test");
        table1.printSchema();
        DataStream<Tuple2<Boolean, Row>> dataStream = tEnv.toRetractStream(table1, Row.class);

        dataStream.map(t->t.f1)

                .map(t->{

//                    System.out.println(1);
                    return t;
                });
//        DataStream<Row> dataStream = tEnv.toAppendStream(table1, Row.class);
        dataStream.print("test");
        env.execute();


    }
}
