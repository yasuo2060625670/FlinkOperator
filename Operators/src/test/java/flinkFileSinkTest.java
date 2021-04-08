//import akka.stream.impl.io.FileSink;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.Partitioner;
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.WindowedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.Types;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//import org.junit.Test;
//
//import java.util.concurrent.TimeUnit;
//
//
///**
// * @author ：zz
// */
//
//public class flinkFileSinkTest {
//
//    @Test
//    public void  test() throws Exception {
//
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings blinkStreamSettings= EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,blinkStreamSettings);
//
//        DataStream<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {
//            int count = 0;
//
//            @Override
//            public void run(SourceContext<Row> ctx) throws Exception {
//                while(true) {
//                    count++;
//                    /**
//                     * 获取abc
//                     */
//
//
//                    ctx.collect(Row.of("接入二网","IPS"));
//                    Thread.sleep(1000);
//
//                    ctx.collect(Row.of("接入一网","WAF"));
//                    Thread.sleep(1000);
//                    ctx.collect(Row.of("接入三网", "WAF"));
//                    Thread.sleep(1000);
//
//                    ctx.collect(Row.of("接入四网", "SkyEye"));
//                    Thread.sleep(1000);
//                    ctx.collect(Row.of("接入五网", "SkyEye"));
//                    Thread.sleep(1000);
//                }
//            }
//            @Override
//            public void cancel() {
//            }
//        });
//        env.enableCheckpointing(1000);
//        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
////        env.from
//
//        DataStream<Row> ds22 = env.fromElements(
//                Row.of("接入一网","no.1"),
//                Row.of("接入二网","no.2"),
//                Row.of("接入三网","no.3"),
//                Row.of("接入四网","no.4")
//
//
//        ).map(map->map)
//                .returns(Types.ROW(new String[]{"name","type"},new TypeInformation[]{Types.STRING(),Types.STRING()}));
//
//        TypeInformation<String> string = Types.STRING();
//        TypeInformation<Object[]> typeInformation = Types.OBJECT_ARRAY(TypeInformation.of(Object.class));
////
////       new MapStateDescriptor<>(
////                "BroadcastState",
////                Row.class,
////                TypeInformation.of(Row.class));
////        BroadcastStream<Integer> ruleBroadcastStream = ds22
////                .broadcast(ruleStateDescriptor);
//
//        DataStream<Row> ds = rowDataStreamSource.map((MapFunction) (row -> row))
//                .returns(Types.ROW(new String[]{"name","COMPONENT"}, new TypeInformation[]{Types.STRING(), Types.STRING()}));
//        SingleOutputStreamOperator<Row> ds2 = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
//            @Override
//            public long extractAscendingTimestamp(Row row) {
//                return Long.MIN_VALUE - 1L;
//            }
//        });
//
//
//        Table table = tEnv.fromDataStream(ds22);
//        Table table1 = tEnv.fromDataStream(ds);
//        tEnv.createTemporaryView("test_b",table);
//        tEnv.createTemporaryView("test_a",table1);
//        Table table11 = tEnv.sqlQuery("select a.*,b.* from test_a a left join test_b b on a.name = b.name");
//        table11.printSchema();
//
//        SingleOutputStreamOperator<Row> map1 = tEnv.toRetractStream(table11, Row.class).map(map -> map.f1);
////        tuple2DataStream.map(map->map.f1).print();
//
//final StreamingFileSink<Row> sink = StreamingFileSink//路径是文件夹
//    .forRowFormat(new Path("C:\\tmp"), new SimpleStringEncoder<Row>("UTF-8"))
//    .withRollingPolicy(
//        DefaultRollingPolicy.builder()
//            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//            .withMaxPartSize(1024 * 1024 * 1024)
//            .build())
//	.build();
////        WindowedStream<Row, Tuple, TimeWindow> window = map1.keyBy("").window(TumblingEventTimeWindows.of(Time.seconds(5)));
////
//
//        map1.partitionCustom(new Partitioner<Row>() {
//            @Override
//            public int partition(Row key, int numPartitions) {
//                if (key.getField(0).equals("接入二网")){
//                    return 1;
//                }else {
//                    return 0;
//                }
//            }
//        }, "name").addSink(sink);
//    map1.keyBy(row->row.getField(0))
//                .timeWindow(Time.seconds(5000),Time.seconds(1000));
//        env.execute();
//    }
//}
//
//
//
//
