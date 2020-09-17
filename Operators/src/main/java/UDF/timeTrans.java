package UDF;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author ：zz
 * @date ：Created in 2020/9/5 11:49
 */
public class timeTrans extends ScalarFunction {

    public Long eval(String time_str,String format) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        return simpleDateFormat.parse(time_str).getTime();

    }
}
