package AccOperator.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;

/**
 * @author ：zz
 * @date ：Created in 2020/9/11 9:07
 */
public class GlobalProcessWindowFunction extends ProcessWindowFunction<Row, Row, Row, GlobalWindow> {
    public  final Logger LOGGER = LoggerFactory.getLogger(GlobalProcessWindowFunction.class);
    private transient ValueState<Tuple2<String, Long>> countState;
    private transient ValueState<Long> state;

    public GlobalProcessWindowFunction() {


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("infostate", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        })));
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));

    }

    @Override
    public void process(Row row, Context context, Iterable<Row> iterable, Collector<Row> collector) throws Exception {

        Iterator<Row> iterator = iterable.iterator();
        System.out.println("process run");
        while (iterator.hasNext()) {

            Row next = iterator.next();
//            LOGGER.info("row = {}" , next.toString());

            if (state.value() == null) {
                state.update(0L);
            }
            if (!next.getField(2).toString().equalsIgnoreCase("已处置")) {
                state.update(state.value() + 1L);
            } else {
                state.update(1L);
            }
            String field = (String)next.getField(3);

            Row of = Row.of(next.getField(0),next.getField(2),  state.value());

            collector.collect(of);
//            collector.collect(next);

        }


    }
}
