package AccOperator.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.types.Row;
import AccOperator.bean.CountAccumulator;

import java.math.BigDecimal;

/**
 * @author ：zz
 * @date ：Created in 2020/6/22 10:40
 */
public class AccFunction implements AggregateFunction<Row,CountAccumulator,CountAccumulator> {
    public  int countIndex ;
    public  int sumIndex ;


    public AccFunction(int countIndex,int sumIndex) {
        this.countIndex = countIndex;
        this.sumIndex = sumIndex;
    }

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator();
    }

    @Override
    public CountAccumulator add(Row row, CountAccumulator countAccumulator) {
        if (countAccumulator.key.isEmpty()){
            countAccumulator.key = (String)row.getField(countIndex);
        }
        //计数
        countAccumulator.count+=1;
        //求和
        countAccumulator.sum2 = countAccumulator.sum2+(Integer) row.getField(sumIndex);
        countAccumulator.sum = countAccumulator.sum.add(new BigDecimal((Integer) row.getField(sumIndex)));

        return countAccumulator;
    }

    @Override
    public CountAccumulator getResult(CountAccumulator countAccumulator) {
        return countAccumulator;
    }

    @Override
    public CountAccumulator merge(CountAccumulator countAccumulator1, CountAccumulator countAccumulator2) {
        CountAccumulator countAccumulatorAll = new CountAccumulator();
        countAccumulatorAll.key = countAccumulator1.key;
        countAccumulatorAll.count = countAccumulator1.count+countAccumulator2.count;
        countAccumulatorAll.sum = countAccumulator1.sum.add(countAccumulator2.sum);
        countAccumulatorAll.sum2 = countAccumulator1.sum2+countAccumulator2.sum2;
        return countAccumulator1;
    }
}
