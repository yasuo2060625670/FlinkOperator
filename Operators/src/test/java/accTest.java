/**
 * @author ：zz
 * @date ：Created in 2020/6/22 10:44
 */
import AccOperator.functions.AccFunction;
import AccOperator.functions.AccProcessWindowFunction;
import AccOperator.functions.TimeIntervalTrigger;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.apache.flink.table.api.Types;

import java.util.Arrays;
import java.util.Date;

/**
 * @author ：zhuwei
 */
public class accTest {
    @Test
    public  void test () throws  Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {
            int count = 0;

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while (true) {
                    count++;
//                    Date date = new Date(System.currentTimeMillis());
//                    String str = "中国银行";
//                    if (count >=10)
//                    {
//                        str = "";
//                    }
//                    ctx.collect(Row.of("192.168.1.1",count, str, date.toString()));
//                    Thread.sleep(1000);
//                    count++;
//                    Date date2 = new Date(System.currentTimeMillis());
//                    ctx.collect(Row.of("192.168.1.2",count, "建设银行", date2.toString()));
//                    Thread.sleep(1000);
//                    count++;
//                    Date date3 = new Date(System.currentTimeMillis());
//                    ctx.collect(Row.of("192.168.1.3",count, "工商银行", date3.toString()));
//                    Thread.sleep(1000);
//                    Date date4 = new Date(System.currentTimeMillis());
//                    ctx.collect(Row.of("192.168.1.2",count, "建设银行", date4.toString()));
//                    Thread.sleep(1000);
//                    Date date5 = new Date(System.currentTimeMillis());
//                    ctx.collect(Row.of("192.168.1.3",count, "工商银行", date5.toString()));
//                    Thread.sleep(1000);
                    long l = System.currentTimeMillis();
                    Date date6 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.1",count, "广发银行", l+5000));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return (Long) row.getField(3);
            }
        });

        env.enableCheckpointing(1000);
        env.setStateBackend(new MemoryStateBackend());
        env.enableCheckpointing(1000);

// 高级选项：
// 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
// 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        SingleOutputStreamOperator<Row> returns = rowDataStreamSource.keyBy(new KeySelector<Row, Row>() {
            @Override
            public Row getKey(Row row) throws Exception {
                int[] arr = {0} ;
                return  Row.project(row,arr);
            }
        })
                .window( SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(60)))
                .trigger(new TimeIntervalTrigger(10000))
                .aggregate(new AccFunction(2, 1),
                        new AccProcessWindowFunction()
                ).returns(Types.ROW(new String[]{"institution_short", "inst_count", "inst_sum"}, new TypeInformation[]{Types.STRING(), Types.LONG(), Types.LONG()}));
        returns.print();
        env.execute();
    }
}
