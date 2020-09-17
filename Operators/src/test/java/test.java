/**
 * @author ：zz
 * @date ：Created in 2020/6/22 10:44
 */
import AccOperator.functions.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.Types;
import org.apache.flink.util.Collector;

import java.util.Date;
/**
 * @author ：zhuwei
 */
public class test {

    public static void main (String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {
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
                    Thread.sleep(1000);
                    count++;
                    Date date2 = new Date(System.currentTimeMillis());
//                    ctx.collect(Row.of("192.168.1.2",0, "未处置", date2.toString()));
//                    Thread.sleep(1000);
                    count++;
                    Date date3 = new Date(System.currentTimeMillis());

                    ctx.collect(Row.of("192.168.1.1",0, "未处置", date3.toString()));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("192.168.1.1",0, "未处置", date3.toString()));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("192.168.1.1",0, "未处置", date3.toString()));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("192.168.1.1",0, "未处置", date3.toString()));
                    Thread.sleep(1000);
                    Date date4 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.2",0, "未处置", date4.toString()));
                    Thread.sleep(1000);
                    Date date5 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.3",0, "未处置", date5.toString()));
                    Thread.sleep(1000);
                    Date date6 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.1",0, "处置中", date6.toString()));
                    Thread.sleep(1000);
                    Date date7 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.2",0, "处置中", date7.toString()));
                    Thread.sleep(1000);
                    Date date8 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.3",0, "处置中", date8.toString()));
                    Thread.sleep(1000);
                    Date date9 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.1",0, "已处置", date9.toString()));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("192.168.1.1",0, "已处置", date9.toString()));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("192.168.1.1",0, "已处置", date9.toString()));
                    Thread.sleep(1000);
                    Date date10 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.2",0, "已处置", date10.toString()));
                    Thread.sleep(1000);
                    Date date11 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.3",0, "已处置", date11.toString()));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {

            }
        });
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Row> ds2 = rowDataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return System.currentTimeMillis();
            }
        });

        /**
         * 1.
         */
        int[] arr = {0};
//ds2.print();
//        ds2.
//        keyBy(row -> Row.project(row, arr))
////                .print();
//            .window(GlobalWindows.create())
//            .trigger(new GlobalWindowTrigger(5000))
//
//                .process(new GlobalProcessWindowFunction())
//                .filter(row->row.getField(0).toString().equalsIgnoreCase("192.168.1.1"))
//                .print();


//        ds2.windowAll(GlobalWindows.create())
//                .trigger(new GlobalWindowTrigger(5000))
//                .process(new GlobalAllProcessWindowFunction())
//                .filter(row->row.getField(0).toString().equalsIgnoreCase("192.168.1.1"))
//                .print();


        ds2.keyBy(row -> row.getField(0))
                .map(new MyRichMapFunction())
                .print();

        env.execute();



    }
}
