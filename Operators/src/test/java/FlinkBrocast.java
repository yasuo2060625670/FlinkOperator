/**
 * @author ：zz
 */

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class FlinkBrocast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Integer> ds1 = env.fromElements(3);
        DataStreamSource<Integer> ds2 = env.fromElements(7, 8, 9, 10);
        MapStateDescriptor<String, Integer> ruleStateDescriptor = new MapStateDescriptor<>(
                "BroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(Integer.TYPE));

        BroadcastStream<Integer> ruleBroadcastStream = ds1
                .broadcast(ruleStateDescriptor);
        ds2.connect(ruleBroadcastStream)
                .process(
                        new BroadcastProcessFunction<Integer, Integer, String>() {

                            @Override
                            public void processElement(Integer integer, ReadOnlyContext readOnlyContext, org.apache.flink.util.Collector<String> collector) throws Exception {
                                ReadOnlyBroadcastState<String, Integer> state = readOnlyContext.getBroadcastState(ruleStateDescriptor);
                                Integer integer1 = state.get("test");
                                collector.collect(integer + "=" + integer1);
                            }
                            @Override
                            public void processBroadcastElement(Integer integer, Context context, Collector<String> collector) throws Exception {
                                context.getBroadcastState(ruleStateDescriptor).put("test", integer);

                            }
                        }
                ).print();
        env.execute();
    }
}