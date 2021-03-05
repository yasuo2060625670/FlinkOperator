import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.Random;

/**
 * @author ：zz
 */
public class ttt {
    private static final ThreadLocal<SimpleDateFormat> MONTH_FORMAT = new ThreadLocal<SimpleDateFormat>() {

        @Override


        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM");
        }
    };

    @Test
    public void test() throws ParseException {
//        StreamingJobGraphGenerator
        final long EVERY_HOUR = 60 * 60 * 1000L;
        final long EVERY_Day = 24 * 60 * 60 * 1000L;
        final long EVERY_min = 60 * 1000L;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat month = new SimpleDateFormat("yyyy-MM");
//        while (true) {
        int i = new Random().nextInt(10);
//            System.out.println(i);
//        long currentTimeMillis = System.currentTimeMillis();
//         currentTimeMillis = currentTimeMillis - (currentTimeMillis + EVERY_HOUR * 8) % 60 * 60 * 1000L;
//        System.out.println(simpleDateFormat.format(currentTimeMillis));
//        for (;;){
////                System.out.println("1");
//            }

//        }

        Calendar cal = Calendar.getInstance();
        cal.add(cal.MONTH, 1);
        SimpleDateFormat dft = new SimpleDateFormat("yyyy-MM");
        long time = cal.getTime().getTime();
        String preMonth = dft.format(cal.getTime());
        String format = dft.format(System.currentTimeMillis());
        long time2 = dft.parse(dft.format(dft.parse(preMonth))).getTime();
        long time1 = dft.parse(format).getTime();
//        System.out.println(time1);
//        System.out.println(time2);


        System.out.println((time2 - time1) / (24 * 60 * 60 * 1000));
        LinkedList<Integer> integers = new LinkedList<>();
        integers.add(1);
        integers.add(2);
        integers.add(5);
        integers.add(4);
        integers.add(2);
        integers.forEach(System.out::println);
        long currentTimeMillis = System.currentTimeMillis();
        long currentTimeMillis2;
        currentTimeMillis = currentTimeMillis - (currentTimeMillis + EVERY_min * 8) % EVERY_min;
        currentTimeMillis2 = currentTimeMillis - (currentTimeMillis + EVERY_min) % EVERY_min;
//        System.out.println(currentTimeMillis);
//        System.out.println(currentTimeMillis2);
//        System.out.println(Long.MAX_VALUE/EVERY_HOUR/24);
        System.out.println("queue1" + simpleDateFormat.format(1619798400000.0));
        System.out.println("queue2" + simpleDateFormat.format(1624982400000.0));
        System.out.println("timetag1" + simpleDateFormat.format(1619798400000.0));
        System.out.println("timetag2" + simpleDateFormat.format(1622476800000.0));

        System.out.println(simpleDateFormat.format(1619798400000.0 + 2678400000.0));
        System.out.println((1622476800000.0 - 1619798400000.0) / EVERY_Day);
//        System.out.println(simpleDateFormat.format(2678400000.0/24/60/60/1000));
        System.out.println(2678400000.0 / EVERY_Day);
        System.out.println(2592000000.0 / EVERY_Day);


        System.out.println(getDayNumOfMonth(currentTimeMillis));
    }

    public long getDayNumOfMonth(long currentTime) {
        try {
            Calendar cal = Calendar.getInstance();
            cal.add(cal.MONTH, -1);

            long currentMonth = MONTH_FORMAT.get().parse(MONTH_FORMAT.get().format(currentTime)).getTime();
            long lastMonth = MONTH_FORMAT.get().parse(MONTH_FORMAT.get().format(cal.getTime().getTime())).getTime();

            return (currentMonth - lastMonth) / (24 * 60 * 60 * 1000);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("");
    }
}
