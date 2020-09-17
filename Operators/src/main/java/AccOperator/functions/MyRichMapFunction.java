package AccOperator.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

/**
 * @author ：zz
 * @date ：Created in 2020/9/11 10:45
 */
public class MyRichMapFunction extends RichMapFunction<Row, Row> {
    private transient MapState<String, Long> state;
    private transient ValueState<Tuple2<String, Long>> infoState;

    public MyRichMapFunction(){


    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = getRuntimeContext().getMapState(new MapStateDescriptor<>("state" , TypeInformation.of(String.class), TypeInformation.of(Long.class)));
        infoState = getRuntimeContext().getState(new ValueStateDescriptor<>("infostate" , TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})));

    }

    @Override
    public Row map(Row row) throws Exception {
//        /*
        String key = row.getField(0).toString();
        if (null == state.get(key)){
            state.put(key,0L);
        }
        String label = row.getField(2).toString();
        if (!label.equalsIgnoreCase("已处置")){
            state.put(key,state.get(key)+1L);
        }else {
            state.put(key,1L);
        }
        return Row.of(row.getField(0),row.getField(2),state.get(key));





    }
}
