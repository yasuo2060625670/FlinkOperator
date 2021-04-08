import AccOperator.functions.DemoProcessFunction;
import AccOperator.functions.TimestampExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * @author ：zz
 * @date ：Created in 2020/8/28 17:00
 */
public class OntimerTest {
    @Test
    public void  test() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> inputStream = env.addSource(new SourceFunction<Row>() {

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while (true) {
                    long time3 = System.currentTimeMillis();
                    ctx.collect(Row.of("jack", 0, time3));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("jack", 0, time3));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Row> rowSingleOutputStreamOperator = inputStream.assignTimestampsAndWatermarks(new TimestampExtractor(Time.seconds(10)));
//        TypeInformation[] typeInformations = {Types.STRING, Types.INT, Types.LONG};
//        DataStream<Row> ds = inputStream.map((MapFunction) (row -> row))
//                .returns(Row.class);
//        ((SingleOutputStreamOperator<Row>) ds).startNewChain();
////        ((SingleOutputStreamOperator<Row>) ds).slotSharingGroup();
        rowSingleOutputStreamOperator
                .map(t->t).setParallelism(1).slotSharingGroup("default")
                .keyBy(t->"")
                .process(new DemoProcessFunction()).setParallelism(1)

//                .slotSharingGroup("test")
//                .startNewChain()//单个算子形成新的任务链
                .disableChaining()//
                .print().setParallelism(1);
        env.setParallelism(1);




        env.execute();
    }
}
class TimestampExtrator extends BoundedOutOfOrdernessTimestampExtractor<Row>{

    public TimestampExtrator(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Row element) {
        return System.currentTimeMillis();
    }
}
