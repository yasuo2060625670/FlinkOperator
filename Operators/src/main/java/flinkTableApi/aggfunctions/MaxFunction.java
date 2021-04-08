package flinkTableApi.aggfunctions;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author ：zz
 */
public class MaxFunction extends AggregateFunction<Integer,MaxAcc> {


    @Override
    public Integer getValue(MaxAcc accumulator) {
        return accumulator.num;
    }

    @Override
    public MaxAcc createAccumulator() {
        return new MaxAcc();
    }

    //类似process
    public void accumulate(MaxAcc accumulator,Integer temp){
       accumulator.num = accumulator.num>=temp?accumulator.num:temp;

    }
}

