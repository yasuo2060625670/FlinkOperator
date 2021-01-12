/**
 * @author ：zz
 * @date ：Created in 2020/6/22 10:44
 */

    import AccOperator.functions.HdfsWriterBucketAssigner;
    import org.apache.flink.api.common.serialization.SimpleStringEncoder;
    import org.apache.flink.api.common.state.ValueState;
    import org.apache.flink.api.common.state.ValueStateDescriptor;
    import org.apache.flink.configuration.Configuration;
    import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
    import org.apache.flink.core.fs.Path;
    import org.apache.flink.streaming.api.TimeCharacteristic;
    import org.apache.flink.streaming.api.datastream.DataStream;
    import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
    import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
    import org.apache.flink.streaming.api.functions.ProcessFunction;
    import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
    import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
    import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
    import org.apache.flink.streaming.api.functions.source.SourceFunction;
    import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
    import org.apache.flink.streaming.api.windowing.time.Time;
    import org.apache.flink.types.Row;
    import org.apache.flink.util.Collector;

    import java.text.SimpleDateFormat;
    import java.util.Date;
    import java.util.concurrent.TimeUnit;

/**
 * @author ：zhuwei
 * 判断数据连续出现
 */

public class testLianXuOutput {

    public static void main (String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {
            int count = 0;

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while (true) {

                    Date date1 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("工商银行","大额系统", "收方交易量", sdf.format(date1)));
                    Thread.sleep(1000);
                    Date date2 = new Date(System.currentTimeMillis());

                    ctx.collect(Row.of("工商银行","大额系统", "收方交易量", sdf.format(date2)));
                    Thread.sleep(1000);

                    Date date3 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("工商银行","大额系统", "收方交易量", sdf.format(date3)));
                    Thread.sleep(1000);

                    Date date4 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("建设银行","大额系统", "收方交易量", sdf.format(date4)));
                    Thread.sleep(1000);

                    Date date5 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("建设银行","大额系统", "收方交易量", sdf.format(date5)));
                    Thread.sleep(1000);

                    Date date6 = new Date(System.currentTimeMillis());
                    ctx.collect(Row.of("工商银行","大额系统", "收方交易量", sdf.format(date6)));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return System.currentTimeMillis();
            }
        });
        env.enableCheckpointing(10000);
        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
        env.setParallelism(10);
        SingleOutputStreamOperator<Row> ds2 = rowDataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return System.currentTimeMillis();
            }
        });
        ds2.keyBy(row->Row.project(row,new int[]{0,1,2}))
        .process(new MyProcessFunction())
        .print();





//        ds2.print();

        env.execute();



    }
}

class MyProcessFunction extends KeyedProcessFunction<Row,Row,Row>{
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private transient ValueState<Long> state;
    private transient ValueState<Long> state2;


    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("",Long.class));
        state2 = getRuntimeContext().getState(new ValueStateDescriptor<>("s",Long.class));


    }

    @Override
    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
        long time = sdf.parse((String) value.getField(3)).getTime();
        if (state.value()==null){
            state.update(time);
            state2.update(1L);
        }else if (time-state.value()<=2000){
            state.update(time);
            state2.update(state2.value()+1L);
        }else {
            state.update(time);
            state2.update(1L);
        }

        Row of = Row.of(value.getField(0), value.getField(1), value.getField(2),value.getField(3), state2.value());
        out.collect(of);



    }
}
class A extends RichSinkFunction<Row>{

    public A() {
        super();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {

    }
}

