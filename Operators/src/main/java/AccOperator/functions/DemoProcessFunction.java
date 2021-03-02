package AccOperator.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @author ：zz
 */
public class DemoProcessFunction extends KeyedProcessFunction<String, Row,Row> {

    private transient ValueState<Boolean> flagState;
    SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Boolean> state = new ValueStateDescriptor<>("", Types.BOOLEAN());
        flagState = getRuntimeContext().getState(state);

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
//            Thread.sleep(100);
//        long timestamp = ctx.timestamp();
        long l = ctx.timerService().currentWatermark();
        long l1 = ctx.timerService().currentProcessingTime();
//        System.out.println("timestamp = "+sdf.format(timestamp));
        System.out.println("Watermark = "+sdf.format(l));
        System.out.println("ProcessingTime = "+sdf.format(l1));


        System.out.println("flag state = "+flagState.value());
        if (flagState.value()==null) {
                ctx.timerService().registerProcessingTimeTimer(l1 + 5000);
            }
        out.collect(value);

    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        System.out.println("+++++++++++++++ on Timer run");
        System.out.println("onTimer timestamp = "+sdf.format(timestamp));

    }
}
