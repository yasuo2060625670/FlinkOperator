import org.apache.flink.table.expressions.In;

import java.util.Optional;

/**
 * @author ：zz
 * @date ：Created in 2020/12/7 14:14
 */
public class OptionalTest {
    /**
     * orElse()null时创建不null也创建
     * orElseGet()不为null 时，不创建对象
     * @param args
     */
    public static void main(String[] args) {
        Integer a = null;
        Integer b = a;
        assert b!=null;
//        Optional<Integer> optional = Optional.empty();
        Integer integer = Optional.ofNullable(b).orElse(0);
        Integer integer2 = Optional.ofNullable(b).orElse(integer);
        Optional.ofNullable(b)
                .filter(t->t>0)
                .isPresent();
//        System.out.println(optional.get());
        System.out.println(integer+2);
        System.out.println(a);
    }
}
