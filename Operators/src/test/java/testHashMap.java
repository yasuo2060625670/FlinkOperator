import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import java.text.SimpleDateFormat.*;
/**
 * @author ：zz
 * @date ：Created in 2020/6/29 16:02
 */
public class testHashMap {
    public static void main(String[] args) {
        HashMap<Integer, Integer> integerIntegerHashMap = new HashMap<>();


        String[][] matrix = new String[2][3];
        matrix[0] = new String[]{"COUNT", "*", "0", "items"};
        matrix[1] = new String[]{"DISTINCT_COUNT", "depart", "0", "departs"};

        List<String[]> strings = Arrays.asList(matrix);
        String a = null;

        if ("".equals(a)){
            System.out.println("1");
        }
         SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        sdf.format("");
    }
}
