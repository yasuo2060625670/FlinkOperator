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
//        System.out.println("str".substring(11));
        log.info("this is info");
        log.error("this is error");
        log.debug("this is debug");
        log.trace("this is trace");
        System.out.println("change_master");

        log.error(String.format("invalid time_str :'%s',invalid row : '%s'", "a", "b"));
        log.error("获取'威胁情报'数据失败，错误码:'%s',错误信息:'%s'", "a", "b");

    }
}

