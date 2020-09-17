package AccOperator.functions;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.text.SimpleDateFormat;

/**
 * @author ：zz
 * @date ：Created in 2020/9/10 17:04
 */
public class GlobalWindowTrigger extends Trigger<Object, GlobalWindow> {

    public long timeInterval;
    ReducingStateDescriptor<Long> timeStateDesc;
    public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public GlobalWindowTrigger(long timeInterval) {

        this.timeInterval = timeInterval;
        timeStateDesc = new ReducingStateDescriptor<Long>("timeInterval", (a, b) -> {
            return a > b ? b : a;
        }, LongSerializer.INSTANCE);

    }
    @Override
    public TriggerResult onElement(Object o, long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> state = ctx.getPartitionedState(timeStateDesc);
        long currentProcessingTime = ctx.getCurrentProcessingTime();
        if (state.get()==null) {
            long start = currentProcessingTime - (currentProcessingTime % timeInterval);
            long nextFireTimestamp = start + timeInterval;
            state.add(nextFireTimestamp);
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow globalWindow, TriggerContext ctx) throws Exception {
        ReducingState<Long> state = ctx.getPartitionedState(timeStateDesc);
        long currentProcessingTime = ctx.getCurrentProcessingTime();
        if (time == state.get()){
            state.clear();
            state.add(time+timeInterval);
            ctx.registerProcessingTimeTimer(time+timeInterval);
            return TriggerResult.FIRE;
        }


        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;

    }

    @Override
    public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

    }
}
