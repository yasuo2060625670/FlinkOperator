import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author ：zz
 */
public class JedisTest {
    Jedis jedis;

    @Before
    public void before() {
        jedis = new Jedis("localhost");
        jedis.auth("root");
        System.out.println(jedis.ping("running"));
    }

    /**
     * String 实例
     */
    @Test
    public void test1() {
        jedis.set("key1", "value1");
        jedis.set("key1", "value2");//
        System.out.println(jedis.get("key1"));
        jedis.del("key1");
        System.out.println(jedis.get("key1"));

    }

    /**
     * keys 实例
     */
    @Test
    public void test2() {
        Set<String> keys = jedis.keys("*");//所有key
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    /**
     * list 实例
     */
    @Test
    public void test3() {
        jedis.lpush("list1", "value1");
        jedis.lpush("list1", "value2");
        jedis.lpush("list1", "value3");
        jedis.lpush("list1", "value4");
//        jedis.flushDB();
        List<String> lrange = jedis.lrange("list1", 0, jedis.llen("list1"));
        lrange.forEach(System.out::println);


    }

    @Test
    public void test4() throws ClassNotFoundException {

        Class.forName("com.mysql.jdbc.Driver");
    }

    @After
    public void after() {
        jedis.close();
    }
}
