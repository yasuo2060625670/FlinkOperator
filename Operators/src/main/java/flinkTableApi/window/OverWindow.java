package flinkTableApi.window;

import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author ：zz
 */
public class OverWindow {
    public static void main(String[] args) throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        StreamExecutionEnvironment env = environment.getEnv();
        StreamTableEnvironment tEnv = environment.gettEnv();

        DataStream<Row> element = environment.getElement();
//        element.print();
        Table table = tEnv.fromDataStream(element,"name,time_str,cn,proctime.proctime,rowtime.rowtime");
        tEnv.createTemporaryView("tmp",table);
        Table table1 = tEnv.sqlQuery("select rowtime,proctime,name,time_str,cn, max(cn) over (partition by name " +
                "order by rowtime " +//必须用时间，
                "RANGe  between interval '2' minute preceding  and current row) as max_cn from tmp");
        DataStream<Row> dataStream = tEnv.toAppendStream(table1, Row.class);
        dataStream.print();
//        Table table1 = table.window(Over.partitionBy("name").orderBy("proctime").preceding("2.minutes").as("w"))
//                .select("name,time_str,cn,cn.max over w");


        dataStream.print();

        tEnv.execute("");
    }
}
