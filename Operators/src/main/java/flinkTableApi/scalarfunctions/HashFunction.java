package flinkTableApi.scalarfunctions;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author ：zz
 */
public class HashFunction extends ScalarFunction {

    public Integer  eval(Object obj){
        return obj.hashCode();
    }
}
