///**
// * @author ：zz
// * @date ：Created in 2020/6/22 10:44
// */
//import AccOperator.functions.*;
//import com.google.common.collect.Lists;
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.Iterator;
//import java.util.concurrent.TimeUnit;
//
///**
// * @author ：zhuwei
// */
//public class test {
//
//    public static void main (String[] args) throws Exception {
//        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {
//            int count = 0;
//
//            @Override
//            public void run(SourceContext<Row> ctx) throws Exception {
//                while (true) {
//
////                    if ()
////                    Thread.sleep(100);
//                    count +=7;
//                    if (count%7000==0){
//                        System.out.println("count = "+count);
//                    }
//
//                    Date date2 = new Date(System.currentTimeMillis());
////                    ctx.collect(Row.of("192.168.1.2",0, "未处置", date2.toString()));
////                    Thread.sleep(1000);
//                    Date date3 = new Date(System.currentTimeMillis());
//
//                    ctx.collect(Row.of("192.168.1.1",0, "未处置", sdf.format(date3)));
////                    Thread.sleep(1000);
//                    ctx.collect(Row.of("192.168.1.1",0, "未处置",sdf.format(date3)));
////                    Thread.sleep(1000);
//                    ctx.collect(Row.of("192.168.1.1",0, "未处置", sdf.format(date3)));
////                    Thread.sleep(1000);
//                    ctx.collect(Row.of("192.168.1.1",0, "未处置", sdf.format(date3)));
////                    Thread.sleep(1000);
//                    Date date4 = new Date(System.currentTimeMillis());
//                    ctx.collect(Row.of("192.168.1.2",0, "未处置", sdf.format(date4)));
////                    Thread.sleep(1000);
//                    Date date5 = new Date(System.currentTimeMillis());
//                    ctx.collect(Row.of("192.168.1.3",0, "未处置", sdf.format(date5)));
////                    Thread.sleep(1000);
//                    Date date6 = new Date(System.currentTimeMillis());
//                    ctx.collect(Row.of("192.168.1.1",0, "处置中", sdf.format(date6)));
////                    Thread.sleep(1000);
////                    Date date7 = new Date(System.currentTimeMillis());
////                    ctx.collect(Row.of("192.168.1.2",0, "处置中", date7.toString()));
////                    Thread.sleep(1000);
////                    Date date8 = new Date(System.currentTimeMillis());
////                    ctx.collect(Row.of("192.168.1.3",0, "处置中", date8.toString()));
////                    Thread.sleep(1000);
////                    Date date9 = new Date(System.currentTimeMillis());
////                    ctx.collect(Row.of("192.168.1.1",0, "已处置", date9.toString()));
////                    Thread.sleep(1000);
////                    ctx.collect(Row.of("192.168.1.1",0, "已处置", date9.toString()));
////                    Thread.sleep(1000);
////                    ctx.collect(Row.of("192.168.1.1",0, "已处置", date9.toString()));
////                    Thread.sleep(1000);
////                    Date date10 = new Date(System.currentTimeMillis());
////                    ctx.collect(Row.of("192.168.1.2",0, "已处置", date10.toString()));
////                    Thread.sleep(1000);
////                    Date date11 = new Date(System.currentTimeMillis());
////                    ctx.collect(Row.of("192.168.1.3",0, "已处置", date11.toString()));
//                    Thread.sleep(1000);
//                }
//            }
//            @Override
//            public void cancel() {
//
//            }
//        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
//            @Override
//            public long extractAscendingTimestamp(Row element) {
//                return System.currentTimeMillis();
//            }
//        });
//        env.enableCheckpointing(10000);
//        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
////
//        env.setParallelism(20);
//        SingleOutputStreamOperator<Row> ds2 = rowDataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
//            @Override
//            public long extractAscendingTimestamp(Row row) {
//                return System.currentTimeMillis();
//            }
//        });
////        BasePathBucketAssigner
////        OnCheckpointRollingPolicy.build()
////                .shouldRollOnProcessingTime()
////        BasePathBucketAssigner<Object> objectBasePathBucketAssigner = new BasePathBucketAssigner<>();
////        DateTimeBucketAssigner<String> stringDateTimeBucketAssigner = new DateTimeBucketAssigner<>("'dt='yyyyMMdd", ZoneId.of("Asia/ShangHai"));
//        final StreamingFileSink<Row> sink = StreamingFileSink//路径是文件夹
////                .forBulkFormat()
//                .forRowFormat(new Path("C:\\tmp"), new SimpleStringEncoder<Row>("UTF-8"))
//                .withBucketAssigner(new HdfsWriterBucketAssigner())
////         .withBucketCheckInterval(DateTimeBucketAssigner) // 1 hour
////                .withRollingPolicy(OnCheckpointRollingPolicy.build()
////
////                )
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(3))
//                                .withInactivityInterval(TimeUnit.HOURS.toMillis(3))
//                                .withMaxPartSize(1024 * 1024 * 128)//==1G 合并
//                                .build())
//                .build();
////Lists
////        ds2.print();
////  [[]\      WatermarkStrategy.forb
//
////        SingleOutputStreamOperator<Row> process = rowDataStreamSource.keyBy(row -> (String) row.getField(0))
////                .timeWindow(Time.days(1))
////                .
////                .trigger(new TimeIntervalTrigger(1000))
////                .
////        process.print();
////        ds2.addSink(sink);
//
//        env.execute();
//
//
//
//    }
//}
//
