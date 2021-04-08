package flinkTableApi.tablefunctions;

import org.apache.flink.table.functions.TableFunction;

/**
 * @author ：zz
 */
public  class SplitFunction extends TableFunction<String> {

    public  void eval(String obj){

        String[] split = obj.toString().split(",");
        for (String s : split) {
            collector.collect(s);
        }

    }

}
