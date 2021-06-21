import AccOperator.functions.TimeIntervalTrigger;
import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Iterator;

/**
 * @author ：zz
 */
public class flinkProcessWindowTest {
    @Test
    public void test() throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        Table table = environment.getTable();
        StreamExecutionEnvironment env = environment.getEnv();
        DataStream<Row> outputStream = environment.getOutputStream();
        outputStream.keyBy(t->(String)t.getField(0))

                .timeWindow(Time.seconds(10))
                .trigger(new TimeIntervalTrigger(2))

                .process(new ProcessWindowFunction<Row, Row, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Row> elements, Collector<Row> out) throws Exception {
                        for (Row element : elements) {
                            out.collect(element);
                        }
                    }
                }).print();
//        StreamTableEnvironment tableEnvironment = environment.gettEnv();
//        Table table1 = tableEnvironment.sqlQuery("select cast( 0 as double) as lons from test");
//        table1.printSchema();


        env.execute();

    }
}
