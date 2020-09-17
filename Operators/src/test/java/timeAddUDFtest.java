import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * @author ：zz
 * @date ：Created in 2020/8/28 17:00
 */
public class timeAddUDFtest {
    @Test
    public void  test() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> inputStream = env.addSource(new SourceFunction<Row>() {

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while (true) {
                    long time3 = System.currentTimeMillis();
                    ctx.collect(Row.of("jack", 0, time3));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        TypeInformation[] typeInformations = {Types.STRING, Types.INT, Types.LONG};
        DataStream<Row> ds = inputStream.map((MapFunction) (row -> row))
                .returns(Row.class);
        ((SingleOutputStreamOperator<Row>) ds).startNewChain();
//        ((SingleOutputStreamOperator<Row>) ds).slotSharingGroup();





        env.execute();
    }
}
