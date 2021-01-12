package AccOperator.functions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import AccOperator.bean.CountAccumulator;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author ：zz
 * @date ：Created in 2020/6/22 16:04
 */
public class AccProcessWindowFunction extends ProcessWindowFunction<CountAccumulator, Row,Row, TimeWindow> {



    @Override
    public void process(Row row, Context context, Iterable<CountAccumulator> iterable, Collector<Row> collector) throws Exception {
        Iterator<CountAccumulator> iterator = iterable.iterator();
        while (iterator.hasNext()){
            CountAccumulator cal = iterator.next();
//            System.out.println("key = "+cal.key);
            collector.collect(Row.of(cal.key,cal.count,cal.sum2));

        }
    }
}
