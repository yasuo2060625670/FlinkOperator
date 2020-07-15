import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;



/**
 * @author ：zz
 * @date ：Created in 2019/10/9 17:09
 */

public class test7 {
//   private static ArrayList<Row> arrayList = new ArrayList<Row>(5);

    @Test
    public void  test() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {
            int count = 0;
            int num = 0;
            long time3 = System.currentTimeMillis();
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while(true) {
                    long time1 = System.currentTimeMillis();
                    count++;

//                    long time2 = System.currentTimeMillis();
//                    ctx.collect(Row.of(null, null, null, null));

                    ctx.collect(Row.of("jack", "10000",    time3, num));
                    long time4 = System.currentTimeMillis();
                    Thread.sleep(1000);


                }
            }

            @Override
            public void cancel() {

            }
        });
//
        env.enableCheckpointing(1000);
        env.setStateBackend(new MemoryStateBackend());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);
        DataStream<Row> ds = rowDataStreamSource.map((MapFunction) (row -> row))
                .returns(Types.ROW(new String[]{"NAME", "HOST", "EVENT_TIME", "NUM"}, new TypeInformation[]{Types.STRING(), Types.STRING(), Types.LONG(), Types.INT()}));
        SingleOutputStreamOperator<Row> ds2 = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Long.MIN_VALUE - 1L;
            }
        });
        long l = System.currentTimeMillis();
        long minValue = Long.MIN_VALUE;
        System.out.println("min = "+ minValue);


        //Caused by: java.lang.RuntimeException: Rowtime timestamp is null. Please make sure that a proper TimestampAssigner is defined and the stream environment uses the EventTime time characteristic.

        Table table = tEnv.fromDataStream(ds2,"NAME,HOST,EVENT_TIME,times.proctime");
        tEnv.registerTable("TEST",table);
        Table table11 = tEnv.sqlQuery("select cast(HOST as int)/1000 from TEST");

        table11.printSchema();
        ds.print();
//        d23.print();
        env.execute();
    }
}




