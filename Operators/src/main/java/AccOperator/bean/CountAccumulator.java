package AccOperator.bean;

import java.math.BigDecimal;


/**
 * @author ：zz
 * @date ：Created in 2020/6/22 11:07
 */
public class CountAccumulator {
    public String key;
    public long count ;
    public BigDecimal sum ;
    public long sum2 ;

    public CountAccumulator() {

        key = "";
        count = 0L;
        sum2 = 0L;
        sum = new BigDecimal(0);

    }

    public CountAccumulator(String key, long count, BigDecimal sum) {
        this.key = key;
        this.count = count;
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "CountAccumulator{" +
                "key = " + key +
                ", count=" + count +
                ", sum2=" + sum +
                '}';
    }
}
