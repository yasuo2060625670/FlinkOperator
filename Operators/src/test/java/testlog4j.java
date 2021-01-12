import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * @author ：zz
 * @date ：Created in 2020/8/28 11:58
 */
@Slf4j
public class testlog4j {
    @Test
    public void  test(){
        System.out.println("str".substring(11));
        log.info("this is info");
        log.error("this is error");
        log.debug("this is debug");
        log.trace("this is trace");
    }
}

