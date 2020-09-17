import UDF.timeTrans;
import org.apache.commons.lang3.StringUtils;
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

public class flinkSqlTest {
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
                    count++;
                    ctx.collect(Row.of("jack", "2020-09-05 11:55:00", 1.9994, num));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        env.enableCheckpointing(1000);
        env.setStateBackend(new MemoryStateBackend());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> ds = rowDataStreamSource.map((MapFunction) (row -> row))
                .returns(Types.ROW(new String[]{"NAME", "HOST", "health_degree", "NUM"}, new TypeInformation[]{Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.INT()}));
        SingleOutputStreamOperator<Row> ds2 = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Long.MIN_VALUE - 1L;
            }
        });



        //Caused by: java.lang.RuntimeException: Rowtime timestamp is null. Please make sure that a proper TimestampAssigner is defined and the stream environment uses the EventTime time characteristic.

        Table table = tEnv.fromDataStream(ds2);
        tEnv.registerTable("TEST",table);
        tEnv.registerFunction("timeTrans", new timeTrans());

        Table table11 = tEnv.sqlQuery("select timeTrans(HOST,'yyyy-MM-dd HH:mm:ss') as a ,cast(substring(cast(health_degree as varchar),1,5) as double) b from TEST");
        table11.printSchema();

        DataStream<Row> dataStream = tEnv.toAppendStream(table11, Row.class);
        dataStream.print();
//        ((SingleOutputStreamOperator<Row>) ds).setParallelism(10).print();
//        ((SingleOutputStreamOperator<Row>) ds).setParallelism(10);
//        d23.print();
        env.execute();
    }
}




