/**
 * @author ：zz
 * @date ：Created in 2020/6/22 10:44
 */
import AccOperator.functions.AccFunction;
import AccOperator.functions.AccProcessWindowFunction;
import AccOperator.functions.TimeIntervalTrigger;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.apache.flink.table.api.Types;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

/**
 * @author ：zhuwei
 */
public class accTest {
    @Test
    public  void test () throws  Exception {

//        ParameterTool params = ParameterTool.fromArgs(args);
//        final boolean printThroughput = params.getBoolean("printThroughput", true);
//        final int port = params.getInt("port", 6124);
//        final int parallelism = params.getInt("parallelism", 4);
//
//        // We use a mini cluster here for sake of simplicity, because I don't want
//        // to require a Flink installation to run this demo. Everything should be
//        // contained in this JAR.
//
//        Configuration config = new Configuration();
//        config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, port);
//        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
//        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, parallelism);



        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createRemoteEnvironment("localhost", 6124, 1);
//        env.getConfig().enableSysoutLogging();

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment  .createRemoteEnvironment("192.168.99.100", 9069);
//        Configuration config = new Configuration();
//        config.setInteger(ConfigOptions.key("rest.port").defaultValue(8081),8081);
//        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
////启用Queryable State服务
//        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
//        Configuration config = new Configuration();
//        config.setInteger(ConfigOptions.key("query.server.ports").defaultValue(8081),8081);
//        config.setInteger("query.server.network-threads", 1);
//        config.setInteger("query.server.query-threads", 1);
//////启用Queryable State服务
////        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

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
                    long l = System.currentTimeMillis();

                    Date date5 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.3",count, "工商银行", l+1000));
                    Thread.sleep(1000);
                    Date date6 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("192.168.1.1",count, "广发银行", l+1000));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return System.currentTimeMillis();
            }
        });
        //false 异步快照关闭
        env.setStateBackend(new FsStateBackend("hdfs://docker31:9000/tmp/checkpoints",false));
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
//        rowDataStreamSource.keyBy(row->(String)row.getField(0))
//                .timeWindow(Time.days(1))
//                .trigger(new TimeIntervalTrigger(1000))
//                .process(new ProcessWindowFunction<Row, Row, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<Row> elements, Collector<Row> out) throws Exception {
//                                Iterator<Row> iterator = elements.iterator();
//                        while (iterator.hasNext()) {
//                            out.collect(iterator.next());
//                        }
//                    }
//                });

        rowDataStreamSource .print();
                
//        SingleOutputStreamOperator<Row> returns = rowDataStreamSource.keyBy(new KeySelector<Row, Row>() {
//            @Override
//            public Row getKey(Row row) throws Exception {
//                int[] arr = {0} ;
//                return  Row.project(row,arr);
//            }
//        })
//                .window( TumblingEventTimeWindows.of(Time.days(1)))
//                .trigger(new TimeIntervalTrigger(10000))
//
//                .aggregate(new AccFunction(2, 1),
//                        new AccProcessWindowFunction()
//                ).returns(Types.ROW(new String[]{"institution_short", "inst_count", "inst_sum"}, new TypeInformation[]{Types.STRING(), Types.LONG(), Types.LONG()}));
//        returns.print();
        env.execute();
    }
}
