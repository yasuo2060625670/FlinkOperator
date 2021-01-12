import AsycOperator.OracleClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author ：zz
 */
public class flinkAsyncioTest {
    public static void main(String[] args) throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        DataStream<Row> outputStream = environment.getOutputStream();
        Table table = environment.getTable();
        StreamExecutionEnvironment env = environment.getEnv();
        SingleOutputStreamOperator<Row> rowSingleOutputStreamOperator =
                AsyncDataStream.orderedWait(outputStream, new CacheAsyncFunction(), 10, TimeUnit.SECONDS, 20);
        rowSingleOutputStreamOperator.print();
        env.execute();
    }

}

class TestAsyncFunction extends RichAsyncFunction<Row, Row> {

    private OracleClient client;

    public TestAsyncFunction() {
        super();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open run");
        client = new OracleClient();

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {

    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        List<Row> query = client.query(input);
        resultFuture.complete(query);
    }
}

class CacheAsyncFunction extends RichAsyncFunction<Row, Row> {
    transient OracleClient client;
    transient LoadingCache<Row, Optional<List<Row>>> build;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new OracleClient();
        System.out.println("open run");
        build = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .refreshAfterWrite(1, TimeUnit.DAYS)
                .build(new CacheLoader<Row, Optional<List<Row>>>() {
                    @Override
                    public Optional<List<Row>> load(Row row) throws Exception {
                        System.out.println("open row = " + row);
                        List<Row> query = client.query(row);

                        return Optional.of(query);
                    }
                });


    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        System.out.println("invoke row = " + input.getField(0));
        System.out.println("build hash = " + build.hashCode());

        List<Row> rows = build.get(Row.of(input.getField(0))).get();
        List<Row> collect = rows.stream().filter(t -> t.getField(1).toString().equals(input.getField(0))).collect(Collectors.toList());
        resultFuture.complete(collect);

    }
}
