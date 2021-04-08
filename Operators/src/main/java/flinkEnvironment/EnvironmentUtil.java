package flinkEnvironment;

import AccOperator.functions.TimestampExtractor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;

/**
 * @author ：zz
 */
public class EnvironmentUtil {
    public static StreamExecutionEnvironment env;
    public static final SimpleDateFormat SDF = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
    public static FlinkEnvironment getEnvironment() {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,blinkStreamSettings);
        DataStream<Row> inputStream = env.addSource(new SourceFunction<Row>() {

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                int i = 0;
                while (true) {
                    long time3 = System.currentTimeMillis();
                    ctx.collect(Row.of("a", 23+i++, time3));
                    Thread.sleep(1000);
                    long time = System.currentTimeMillis();

                    ctx.collect(Row.of("b", 43, time));
                    Thread.sleep(5000);

                    ctx.collect(Row.of("c", 43, time));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        })
                .returns(Types.ROW_NAMED(new String[]{"name","age","time_str"},Types.STRING,Types.INT,Types.LONG))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        return (Long) element.getField(2);
                    }
                });

        SingleOutputStreamOperator<Row> element = env.fromElements(
                Row.of("工商银行", "2017-11-11 10:01:00", 20),
                Row.of("工商银行", "2017-11-11 10:02:00", 30),
                Row.of("工商银行", "2017-11-11 10:03:00", 30),
                Row.of("工商银行", "2017-11-11 10:03:00", 40),
                Row.of("工商银行", "2017-11-11 10:03:00", 50),
                Row.of("工商银行", "2017-11-11 10:04:00", 60),
                Row.of("工商银行", "2017-11-11 10:05:00", 70),
                Row.of("工商银行", "2017-11-11 10:06:00", 20),
                Row.of("工商银行", "2017-11-11 10:07:00", 20),
                Row.of("工商银行", "2017-11-11 10:08:00", 30),
                Row.of("建设银行", "2017-11-11 10:06:00", 20),
                Row.of("建设银行", "2017-11-11 10:08:00", 40)
        ).returns(Types.ROW_NAMED(new String[]{"name", "time_str","cn"}, new TypeInformation[]{Types.STRING, Types.STRING, Types.INT}))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(10)) {
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(Row element) {
                        return SDF.parse(element.getField(1).toString()).getTime();
//                        return System.currentTimeMillis();
                    }
                });
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Row> rowSingleOutputStreamOperator;
        rowSingleOutputStreamOperator = inputStream
                .assignTimestampsAndWatermarks(new TimestampExtractor(Time.seconds(10)));
        Table table = tEnv.fromDataStream(inputStream, "name,age,time_str as timestamp,proc_time.proctime,row_time.rowtime");

        tEnv.createTemporaryView("test", table);
        return new FlinkEnvironment()
                .setOutputStream(rowSingleOutputStreamOperator)
                .setEnv(env)
                .settEnv(tEnv)
                .setTable(table)
                .setElement(element);


    }


}
