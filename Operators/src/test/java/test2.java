//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.queryablestate.client.QueryableStateClient;
//import scala.Tuple2;
//
//import java.util.concurrent.CompletableFuture;
//
///**
// * @author ：zz
// * @date ：Created in 2020/11/3 9:23
// */
//public class test2 {
//    public static void main(String[] args) {
//        QueryableStateClient client = new QueryableStateClient(tmHostname, proxyPort);
//
//// the state descriptor of the state to be fetched.
//        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
//                new ValueStateDescriptor<>(
//                        "average",
//                        TypeInformation.of().of(new TypeHint<T>() {}));
//
//        CompletableFuture<ValueState<Tuple2<Long, Long>>> resultFuture =
//                client.getKvState(jobId, "query-name", key, BasicTypeInfo.LONG_TYPE_INFO, descriptor);
//
//// now handle the returned value
//        resultFuture.thenAccept(response -> {
//            try {
//                Tuple2<Long, Long> res = response.get();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//    }
//}
