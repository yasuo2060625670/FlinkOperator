package flinkEnvironment;

import AccOperator.functions.TimestampExtractor;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.lang.reflect.Type;

/**
 * @author ：zz
 */
public class EnvironmentUtil {

    public static FlinkEnvironment getEnvironment() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> inputStream = env.addSource(new SourceFunction<Row>() {

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while (true) {
                    long time3 = System.currentTimeMillis();
                    ctx.collect(Row.of("贝怀瑶", 23, time3));
                    Thread.sleep(1000);
                    long time = System.currentTimeMillis();

                    ctx.collect(Row.of("蔡起运", 43, time));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Row> rowSingleOutputStreamOperator;
        rowSingleOutputStreamOperator = inputStream
                .assignTimestampsAndWatermarks(new TimestampExtractor(Time.seconds(10)))
                .returns(Types.ROW_NAMED(new String[]{"name", "age", "time_str"}, Types.STRING, Types.INT, Types.LONG));
        Table table = tEnv.fromDataStream(rowSingleOutputStreamOperator);
        tEnv.createTemporaryView("test", table);
        return new FlinkEnvironment()
                .setOutputStream(rowSingleOutputStreamOperator)
                .setEnv(env)
                .settEnv(tEnv)
                .setTable(table);


    }


}
