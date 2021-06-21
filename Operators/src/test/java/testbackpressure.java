import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * @author ：zz
 */
public class testbackpressure {

    @Test
    public void test() throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        StreamExecutionEnvironment env = environment.getEnv();

        DataStream<Row> outputStream = environment.getOutputStream();
        outputStream.keyBy(t->t.getField(1)).map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                Thread.sleep(5000);
                return value;
            }
        }).print();
//        GenericWriteAheadSink
//        Histogram
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
    env.execute("zhuwei");

    }
}
