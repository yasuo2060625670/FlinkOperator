package AccOperator.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author ：zz
 * @date ：Created in 2020/9/11 9:48
 */
public class GlobalAllProcessWindowFunction extends ProcessAllWindowFunction<Row, Row, GlobalWindow> {

    private transient ValueState<Tuple2<String, Long>> countState;
    private transient ValueState<Long> state;

    public GlobalAllProcessWindowFunction() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("infostate", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        })));
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));

    }
    @Override
    public void process(Context context, Iterable<Row> iterable, Collector<Row> collector) throws Exception {

        Iterator<Row> iterator = iterable.iterator();

        while (iterator.hasNext()){
            Row row = iterator.next();
            System.out.println("row = "+row);
            if (state.value() == null) {
                state.update(0L);
            }
            if (!row.getField(2).toString().equalsIgnoreCase("已处置")) {
                state.update(state.value() + 1L);
            } else {
                state.update(1L);
            }
            Row of = Row.of(row.getField(0), row.getField(2), state.value());
            collector.collect(of);
        }


    }
}
