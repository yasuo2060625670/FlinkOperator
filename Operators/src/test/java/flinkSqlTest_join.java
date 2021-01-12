import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
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
        env.enableCheckpointing(1000);
        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.from

        DataStream<Row> ds22 = env.fromElements(
                Row.of("接入一网","no.1"),
                Row.of("接入二网","no.2"),
                Row.of("接入三网","no.3"),
                Row.of("接入四网","no.4")


        ).map(map->map)
                .returns(Types.ROW(new String[]{"name","type"},new TypeInformation[]{Types.STRING(),Types.STRING()}));



        DataStream<Row> ds = rowDataStreamSource.map((MapFunction) (row -> row))
                .returns(Types.ROW(new String[]{"name","COMPONENT"}, new TypeInformation[]{Types.STRING(), Types.STRING()}));
        SingleOutputStreamOperator<Row> ds2 = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Long.MIN_VALUE - 1L;
            }
        });


        Table table = tEnv.fromDataStream(ds22);
        Table table1 = tEnv.fromDataStream(ds);
        tEnv.createTemporaryView("test_b",table);
        tEnv.createTemporaryView("test_a",table1);
        Table table11 = tEnv.sqlQuery("select a.*,b.* from test_a a left join test_b b on a.name = b.name");
        table11.printSchema();

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(table11, Row.class);
        tuple2DataStream.map(map->map.f1).print();
//
        env.execute();
    }
}




