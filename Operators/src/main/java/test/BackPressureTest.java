package test;

import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * @author ：zz
 */
public class BackPressureTest  {
    public static void main(String[] args) throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        DataStream<Row> outputStream = environment.getOutputStream();


        outputStream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {


                Thread.sleep(1000);
                return  value;
            }
        });
        StreamExecutionEnvironment env = environment.getEnv();

//        Field field = env.getClass().getDeclaredField("configuration");
//        field.setAccessible(true);
//        StreamExecutionEnvironment  o = (StreamExecutionEnvironment )field.get(env);

//        field.set("rest.bind-port","8081");
        env.execute();
//        Optional.ofNullable()


    }
}
