import java.text.SimpleDateFormat;

/**
 * @author ：zz
 */
public class testtime {
    public static void main(String[] args) {

        ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<SimpleDateFormat>(){
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat("8yyyy-MM-dd HH:mm:ss");
            }
        };
        String format = sdf.get().format(System.currentTimeMillis());
        System.out.println(format);
        System.out.println("change");
        System.out.println("change_branch");


    }
}
