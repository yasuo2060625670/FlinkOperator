package AccOperator.functions;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ：zz
 * @date ：Created in 2020/6/22 13:58
 */
public class TimeIntervalTrigger extends Trigger<Object, TimeWindow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeIntervalTrigger.class);

    public long timeInterval;
    ReducingStateDescriptor<Long> timeStateDesc;
    public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");




    public TimeIntervalTrigger(long timeInterval) {

        this.timeInterval = timeInterval;
        timeStateDesc = new ReducingStateDescriptor<Long>("timeInterval", (a, b) -> {
            return a > b ? b : a;
        }, LongSerializer.INSTANCE);

    }

    @Override
    public TriggerResult onElement(Object o, long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            System.out.println("111");
            return TriggerResult.FIRE;
        }
//        System.out.println((
//                "time = " + SIMPLE_DATE_FORMAT.format(new Date(time)) +
//                        " window start time = " + SIMPLE_DATE_FORMAT.format(new Date(window.getStart())) +
//                        " window end time = " + SIMPLE_DATE_FORMAT.format(new Date(window.getEnd()))));
//        System.out.println(("onElement excute"));
        //存储每次触发时间
        ReducingState<Long> state = ctx.getPartitionedState(timeStateDesc);
//        System.out.println("state::"+(state==null));
        //窗口处理时间
        long currentProcessingTime = ctx.getCurrentProcessingTime();
//        System.out.println(window.getEnd() - timeInterval +"::::"+ currentProcessingTime);
//        System.out.println(window.getEnd() - timeInterval>currentProcessingTime);
        long l = window.getEnd() - timeInterval;
        if (state.get()==null) {
//            System.out.println((
//                    "time = " + SIMPLE_DATE_FORMAT.format(new Date(time)) +
//                            " window start time = " + SIMPLE_DATE_FORMAT.format(new Date(window.getStart())) +
//                            " window end time = " + SIMPLE_DATE_FORMAT.format(new Date(window.getEnd()))));
//
//            System.out.println(window.getEnd() - 10000 +"::::"+ currentProcessingTime);
            System.out.println("on element excute");

            long start = currentProcessingTime - (currentProcessingTime % timeInterval);
            long nextFireTimestamp = start + timeInterval;
//            System.out.println(("onElement excute twice time = "
//                    +SIMPLE_DATE_FORMAT.format(new Date(currentProcessingTime))+" next fire time = "
//                    +SIMPLE_DATE_FORMAT.format(new Date(start+timeInterval))));
//            System.out.println("nextFireTimestamp = "+SIMPLE_DATE_FORMAT.format(new Date(start))+" nextFile = "+SIMPLE_DATE_FORMAT.format(new Date(nextFireTimestamp)));
            state.add(nextFireTimestamp);
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println(("onProcessingTime excute"));

//        System.out.println((
//                "time = " + SIMPLE_DATE_FORMAT.format(new Date(time)) +
//                        " window start time = " + SIMPLE_DATE_FORMAT.format(new Date(window.getStart())) +
//                        " window end time = " + SIMPLE_DATE_FORMAT.format(new Date(window.getEnd()))));

        long windowEnd = window.getEnd();
        ReducingState<Long> state = ctx.getPartitionedState(timeStateDesc);
        if (time == windowEnd){
//            System.out.println("window end ,so fire");
            return TriggerResult.FIRE;
        }else if (time == state.get()){
//            System.out.println("print once");
//            System.out.println("trigger time = "+SIMPLE_DATE_FORMAT.format(new Date(time)));
            //当窗口时间到达下次触发时间时,注册下次触发计时器/记录下次触发时间
            state.clear();
            state.add(time+timeInterval);
            ctx.registerProcessingTimeTimer(time+timeInterval);
//            System.out.println("state =="+(state==null));
            return TriggerResult.FIRE;
        }


        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        long windowEnd = window.getEnd();
        ReducingState<Long> state = ctx.getPartitionedState(timeStateDesc);
        if (time == windowEnd){
            return TriggerResult.FIRE;
        }else if (time == state.get()){
//            System.out.println("trigger time = "+SIMPLE_DATE_FORMAT.format(new Date(time)));
            //当窗口时间到达下次触发时间时,注册下次触发计时器/记录下次触发时间
            state.clear();
            state.add(time+timeInterval);
            ctx.registerEventTimeTimer(time+timeInterval);
            return TriggerResult.FIRE;
        }


        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }
}
