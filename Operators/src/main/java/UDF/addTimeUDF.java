package UDF;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ：zz
 */
public class addTimeUDF extends ScalarFunction {
    final static SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:SS:mm");
    public String eval(){

        return sdf.format(new Date());
    }



}
