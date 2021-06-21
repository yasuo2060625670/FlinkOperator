import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;


/**
 * @author ：zz
 */

public class flinkSqlTest_join {

    @Test
    public void  test() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,blinkStreamSettings);

        DataStream<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {
            int count = 0;

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while(true) {
                    count++;
                    /**
                     * 获取abc
                     */


                    ctx.collect(Row.of("接入二网","IPS"));
                    Thread.sleep(1000);

                    ctx.collect(Row.of("接入一网","WAF"));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("接入三网", "WAF"));
                    Thread.sleep(1000);

                    ctx.collect(Row.of("接入四网", "SkyEye"));
                    Thread.sleep(1000);
                    ctx.collect(Row.of("接入五网", "SkyEye"));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {
            }
        });
//        env.enableCheckpointing(1000);
//        final StateBackend rocksDBStateBackend =
//        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.from

        DataStream<Row> ds22 = env.fromElements(
                Row.of("接入一网","no.1"),
                Row.of("接入二网","no.2"),
                Row.of("接入三网","no.3"),
                Row.of("接入四网","no.4")


        ).map(map->map)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Row element) {

                        return System.currentTimeMillis();
                    }
                })
                .returns(Types.ROW(new String[]{"name","type"},new TypeInformation[]{Types.STRING(),Types.STRING()}));



        DataStream<Row> ds = rowDataStreamSource.map((MapFunction) (row -> row))
                .returns(Types.ROW(new String[]{"name","component"}, new TypeInformation[]{Types.STRING(), Types.STRING()}));
        SingleOutputStreamOperator<Row> ds2 = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return System.currentTimeMillis();
            }
        });


        Table table = tEnv.fromDataStream(ds22,"name1,type1,ltime.rowtime");
        Table table1 = tEnv.fromDataStream(ds2,"name2,component2,rtime.rowtime");
        tEnv.createTemporaryView("test_a",table);
        tEnv.createTemporaryView("test_b",table1);
        Table table11 = table.join(table1)
                .where("name1 = name2 && ltime >= rtime - 5.minutes && ltime < rtime + 10.minutes")
                .select("name1,type1,rtime");
        tEnv.createTemporaryView("test_c",table11);
        tEnv.sqlQuery("select name1,type1,date_format(rtime,'YYYY-MM-dd') as `time` from test_c ");
//
//                Table table11 = tEnv.sqlQuery("select a.name,b.name,b.component,rtime from test_a a inner join test_b b on a.name = b.name ");
//        Table table11 = tEnv.sqlQuery("select a.*,b.* from test_a a inner join test_b b on a.name = b.name");
//        Table table11 = tEnv.sqlQuery("select a.*,b.* from test_a a left join test_b b on a.name = b.name");
        table11.printSchema();

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(table11, Row.class);
        tuple2DataStream.map(map->map.f1).print();
//
        env.execute();
    }
}




