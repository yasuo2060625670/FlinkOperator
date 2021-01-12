import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

public class QueryState {
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        QueryableStateClient client = new QueryableStateClient(
                "localhost" , // taskmanager的地址
                9069);// 默认是9069端口，可以在flink-conf.yaml文件中配置
        // the state descriptor of the state to be fetched.
//        ReducingStateDescriptor<Long> descriptor =
//                new ReducingStateDescriptor<Long(
//                        "timeInterval",
//                        TypeInformation.of(new TypeHint<Long>() {}));
        ReducingStateDescriptor<Long> descriptor  = new ReducingStateDescriptor<Long>("timeInterval", (a, b) -> {
            return a > b ? b : a;
        }, LongSerializer.INSTANCE);
//        timeStateDesc.setQueryable("query_name");

        while (true) {

            CompletableFuture<ReducingState<Long>> resultFuture =



                    client.getKvState(
                            JobID.fromHexString("3c2e26160bc816aa4e15e81e45908f7d"), // 从webui中获取JID
                            "query_name", // wordcount中设置的名字 descriptor.setQueryable("query123");
                            0L, // key的值
                            BasicTypeInfo.LONG_TYPE_INFO, // key 的类型
                            descriptor);

            // now handle the returned value
            resultFuture.thenAccept(response -> {
                try {
                    Long aLong = response.get();

                    System.out.println("value: " + aLong);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            Thread.sleep(1000);
            System.out.println("Run");
        }
    }
}
 