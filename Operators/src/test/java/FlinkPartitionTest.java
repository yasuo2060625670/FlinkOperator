import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * @author ：zz
 */
public class FlinkPartitionTest {
    @Test
    public void  test() throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        DataStream<Row> outputStream = environment.getOutputStream();
        StreamExecutionEnvironment env = environment.getEnv();
//        outputStream.keyBy(t->t.getField(0)).print("keyby");
//        outputStream.shuffle().print("shuffer");
//        outputStream.rebalance().print("reblance");
//        outputStream.partitionCustom()
        env.execute();
    }
}
class MyPartition implements Partitioner<Row> {


    @Override
    public int partition(Row key, int numPartitions) {
        return 0;
    }
}
