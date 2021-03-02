import AccOperator.bean.PoJo;
import flinkEnvironment.EnvironmentUtil;
import flinkEnvironment.FlinkEnvironment;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author ：zz
 */
public class flinkDataTypesTest {
    public static void main(String[] args) throws Exception {
        FlinkEnvironment environment = EnvironmentUtil.getEnvironment();
        DataStream<Row> outputStream = environment.getOutputStream();
        Table table = environment.getTable();
//        environment.c

        StreamExecutionEnvironment env = environment.getEnv();
        outputStream.map(t -> t)
                .returns(Types.ROW());

        //basic/tuple/row
        Types.TUPLE(Types.ROW(), Types.STRING, Types.STRING);
        //pojo
        TypeInformation<PoJo> pojo = Types.POJO(PoJo.class);
        //TypeHit
        TypeInformation<PoJo> of = TypeInformation.of(new TypeHint<PoJo>() {
            @Override
            public TypeInformation<PoJo> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
//Types.ROW_NAMED(new t)
        /**
         * DataTypes/DataType
         *
         */
        DataType string = DataTypes.STRING();
        DataType[] fieldDataTypes = table.getSchema().getFieldDataTypes();
        env.execute();
    }

    public static <T extends Tuple> TypeInformation<T> TUPLE(TypeInformation<?>... types) {
        return new TupleTypeInfo<>(types);
    }

    public TypeInformation<String> String() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}



