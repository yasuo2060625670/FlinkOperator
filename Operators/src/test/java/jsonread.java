import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author ：zz
 */
public class jsonread {
    public static void main(String[] args) throws IOException {
        String s = FileUtils.readFileToString(new File("C:\\shell\\MyProject\\FlinkOperator\\Operators\\src\\main\\resources\\test"));

        String[] split = s.split("=");
        for (int i = 0; i < split.length; i++) {
            if (i%2==0){
                System.out.println(split[i]);
            }else {

            }


        }
    }
}
