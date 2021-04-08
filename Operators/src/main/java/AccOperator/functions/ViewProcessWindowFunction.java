package AccOperator.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author ：zz
 * @date ：Created in 2020/12/7 11:27
 */
public class ViewProcessWindowFunction extends ProcessWindowFunction<Row, Row, String, TimeWindow> {
    private transient ValueState<Integer> state;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", TypeInformation.of(Integer.class)));

    }

    public ViewProcessWindowFunction() {
        super();
    }

    /**
     * 1.state为null，表示是当天过来得最新一条数据，然后将cn信息保存进状态
     * 2.以后得每一条数据,如果 cn == 1 则输出。
     *
     * @param key
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */

    @Override
    public void process(String key, Context context, Iterable<Row> elements, Collector<Row> out) throws Exception {
        Iterator<Row> iterator = elements.iterator();

        while (iterator.hasNext())
       {
           Row next = iterator.next();
           Number cn = (Number) next.getField(4);
           if (state.value() == null) {
               state.update(cn.intValue());
//            System.out.println(sta);
               System.out.println("初始化:key = "+key+" state = "+state.value());
           }
           System.out.println("key = "+key+" state = "+state.value());

           if(state.value()==1) {
               out.collect(next);
           }

       }


    }

    @Override
    public void clear(Context context) throws Exception {
        super.clear(context);
    }
}
