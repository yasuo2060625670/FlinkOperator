import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;

/**
 * @author ：zz
 */
public class testObjectStream {
    @Test
    public void  test() throws IOException {
        int a =0;
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("C:\\shell\\apache-tomcat-9.0.39\\webapps\\web-stage.war"));
        do {
            oos.writeInt(12345);
            oos.writeObject("Today");
            oos.writeObject(new Date());
        }while (2>a);


        oos.close();

    }
}
