package test;

import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

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


                Thread.sleep(1000l);
                return  value;
            }
        });
        StreamExecutionEnvironment env = environment.getEnv();
        env.execute();
//        Optional.ofNullable()


    }
}
