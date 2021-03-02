package AccOperator.functions;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.internal.Trees;

import java.util.Map;

/**
 * @author ：zz
 * @date ：Created in 2020/8/22 17:46
 */
public class UnionProcessFunction extends KeyedProcessFunction<String,Tuple2<String, Row>, Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnionProcessFunction.class);

    private transient ValueState<Long> count;
    private transient ValueState<Long> info;

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }

    private long info_count = 0;
    private String instanceId;
    private RowTypeInfo rowTypeInfo;
    public UnionProcessFunction(RowTypeInfo rowTypeInfo, String instanceId){
        this.instanceId = instanceId;
        this.rowTypeInfo =rowTypeInfo;
    }

    /**
     * 1.info... count+1
     * 2.count...取值
     * 3.
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count"+instanceId , Long.class));
        info = getRuntimeContext().getState(new ValueStateDescriptor<>("info"+instanceId , Long.class));
    }

    /**
     * 拿到info，每条数据加1
     * @param input
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(Tuple2<String, Row> input, Context context, Collector<Row> collector) throws Exception {

        if (input.f0.equals("count")) {
            if (count.value()==null){
                count.update((Long) input.f1.getField(1));
//                System.out.println("count"+input.f1.getField(1));
            }
        } else if (input.f0.equals("info")) {
            info_count++;
            info.update(info_count);
//            System.out.println("info_count"+input.f1.getField(1));
//            System.out.println("info_count = "+ info_count);
        } else {
            throw  new RuntimeException("datasteam error");
        }


        if(!info.value().equals(count.value())){
//            System.out.println("info state = "+info.value());
//            System.out.println("count state = "+count.value());
            collector.collect(input.f1);
        }else {
            count.clear();
            info_count=0;

            System.out.println("add fake and set count is null");
            System.out.println("add fake and set count is null");
            if (input.f0.equalsIgnoreCase("info")) {
                collector.collect(input.f1);
            }
            collector.collect(makeFakeData(input.f1));

        }



    }
    private Row makeFakeData(Row inputRow) {
        Row row = new Row(rowTypeInfo.getArity());
        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        for (int i = 0; i < fieldTypes.length; i++) {
            TypeInformation<?> fieldType = fieldTypes[i];
            if (fieldType.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
                row.setField(i, "fakedata");
            } else if (fieldType.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
                row.setField(i, 0L);
            } else if (fieldType.equals(BasicTypeInfo.INT_TYPE_INFO)) {
                row.setField(i, 0);
            } else if (fieldType.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
                row.setField(i, 0);
            }
        }
        return row;
    }
}
