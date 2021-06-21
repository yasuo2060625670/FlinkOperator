import AccOperator.bean.Event;
import AccOperator.functions.DemoProcessFunction;
import AccOperator.functions.TimestampExtractor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 * @author ：zz
 * @date ：Created in 2020/8/28 17:00
 */
public class flinkCEPTest {
    @Test
    public void  test() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        DataStream<Row> inputStream = env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while (true) {//aaabb
                    long time3 = System.currentTimeMillis();
                    ctx.collect(Row.of("jack", 0, sdf.format(time3)));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("jack2", 0, sdf.format(System.currentTimeMillis())));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("jack3", 0, sdf.format(System.currentTimeMillis())));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("bob", 80, sdf.format(System.currentTimeMillis())));
                    Thread.sleep(1000);
//                    ctx.collect(Row.of("bob2", 90, sdf.format(System.currentTimeMillis())));
//                    Thread.sleep(1000);
//                    ctx.collect(Row.of("bob3", 90, sdf.format(System.currentTimeMillis())));
//                    Thread.sleep(1000);
//                    ctx.collect(Row.of("jack", 100, sdf.format(System.currentTimeMillis())));
//                    Thread.sleep(1000);
//                    ctx.collect(Row.of("bob", 90, sdf.format(System.currentTimeMillis())));
//                    Thread.sleep(1000);
//                    ctx.collect(Row.of("jack2", 100, sdf.format(System.currentTimeMillis())));
//                    Thread.sleep(1000);
//                    ctx.collect(Row.of("bob2", 80, sdf.format(System.currentTimeMillis())));
//                    Thread.sleep(1000);

                }
            }

            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(new TimestampExtractor(Time.milliseconds(0)));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * start:name = jack
         */
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToLast("start");

        Pattern<Row, ?> pattern = Pattern.<Row>begin("start",skipStrategy)
               .where(
                new SimpleCondition<Row>() {
                    @Override
                    public boolean filter(Row event) {
                        return event.getField(0).toString().startsWith("jack");
                    }
                }
               ).followedByAny("middle").subtype(Row.class).where(new SimpleCondition<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                return Integer.parseInt(value.getField(1).toString())>=80;
            }
                }).within(Time.milliseconds(10000));
//        ListCheckpointed
//        CheckpointedFunction

//                .next("middle").subtype(Event.class).where(subEvt -> subEvt.getVolume() >= 10.0)
//                .followedBy("end").where(evt -> evt.getName().equals("end"));
//                }).followedBy("end").where(new FilterFunction<Row>() {
//                    @Override
//                    public boolean filter(Row value) throws Exception {
//                        return value.getName().equals("critical");
//                    }
//                }).within(Time.seconds(10));
        PatternStream<Row> patternStream = CEP.pattern(inputStream, pattern);

//通过select 方法对模式流进行处理
        patternStream.select((PatternSelectFunction<Row, String>) pattern1 -> {
            for (Map.Entry<String, List<Row>> stringListEntry : pattern1.entrySet()) {
                System.out.print(stringListEntry.getKey()+" ");
                stringListEntry.getValue().forEach(System.out::println);
            }
            return "aaa";
        });


        env.execute();
    }
}
