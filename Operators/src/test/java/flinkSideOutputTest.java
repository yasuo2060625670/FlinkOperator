import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author ：zz
 */
public class flinkSideOutputTest {
    public static void main(String[] args) throws Exception {
        FlinkEnvironment flinkEnvironment = EnvironmentUtil.getEnvironment();
        Table table = flinkEnvironment.getTable();
        StreamExecutionEnvironment env = flinkEnvironment.getEnv();
        DataStream<Row> outputStream = flinkEnvironment.getOutputStream();
        final OutputTag<String> tag = new OutputTag<String>("test") {
        };
        SingleOutputStreamOperator<Row> process = outputStream.process(new ProcessFunction<Row, Row>() {

            @Override
            public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
                out.collect(value);//主要输出
                ctx.output(tag, "sideOut" + value);//旁路输出
            }
        });
        DataStream<String> sideOutput = process.getSideOutput(tag);
        sideOutput.timeWindowAll(Time.seconds(5))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(1)))
                .allowedLateness(Time.seconds(1));//允许延迟
//                .evictor(DeltaEvictor.)

        sideOutput.print();

        env.execute();
        String s = " sdf ";
        s.trim();

    }
}

class MyEvictor implements Evictor {

    @Override
    public void evictBefore(Iterable elements, int size, Window window, EvictorContext evictorContext) {

    }

    @Override
    public void evictAfter(Iterable elements, int size, Window window, EvictorContext evictorContext) {

    }
}