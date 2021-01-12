import AccOperator.functions.TimeIntervalTrigger;
import AccOperator.functions.ViewProcessWindowFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;


/**
 * @author ：zz
 */

public class flinkSqlTest2 {

    @Test
            public void test() throws Exception {

    /**1.创建环境->拿到env操作环境对象
     * 2.拿到表tEnv操作环境对象
     *
     *
     * env.addSource();->flink得数据源
     *
     *
     * @throws Exception
     */

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            while (true) {                    Thread.sleep(1000);
                Date date3 = new Date(System.currentTimeMillis());
                ctx.collect(Row.of("192.168.1.1","192.168.1.1",  "a","未处置",1, date3.toString()));
                Thread.sleep(1000);
                ctx.collect(Row.of("192.168.1.2","192.168.1.1", "b","未处置",1, date3.toString()));
                Thread.sleep(1000);
                ctx.collect(Row.of("192.168.1.1","192.168.1.1",  "a","未处置",5, date3.toString()));
                Thread.sleep(1000);
                ctx.collect(Row.of("192.168.1.2","192.168.1.1", "b","未处置",13, date3.toString()));
                Thread.sleep(1000);
                ctx.collect(Row.of("192.168.1.3","192.168.1.1", "b","未处置",1, date3.toString()));
                Thread.sleep(1000);
                ctx.collect(Row.of("192.168.1.4","192.168.1.1", "b","未处置",21, date3.toString()));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {

        }
    });


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> ds = rowDataStreamSource.map((MapFunction) (row -> row))
            .returns(Types.ROW(new String[]{"ip1", "ip2", "field","label","cn","time_str"}, new TypeInformation[]{Types.STRING(),Types.STRING(), Types.STRING(), Types.STRING(),Types.INT(),Types.STRING()}));

    ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
        @Override
        public long extractAscendingTimestamp(Row element) {
            return System.currentTimeMillis();
        }
    })
            .keyBy(map->(String)map.getField(0)
                    



            )
            .window( TumblingEventTimeWindows.of(Time.days(1),Time.hours(16)))
            .trigger(new TimeIntervalTrigger(5000))
            .process(new ViewProcessWindowFunction())
            .print();
//ds.keyBy("").process()
        TypeInformation<String> of = TypeInformation.of(String.class);

        if (BasicTypeInfo.STRING_TYPE_INFO==TypeInformation.of(String.class))

                env.execute();
    }
}




