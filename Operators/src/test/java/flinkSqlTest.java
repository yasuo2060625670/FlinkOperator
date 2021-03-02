import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;


/**
 * @author ：zz
 */

public class flinkSqlTest {

    /**1.创建环境->拿到env操作环境对象
     * 2.拿到表tEnv操作环境对象
     *
     *
     * env.addSource();->flink得数据源
     *
     *
     * @throws Exception
     */
    @Test
    public void  test() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,blinkStreamSettings);

        DataStream<Row> rowDataStreamSource = env.addSource(new SourceFunction<Row>() {
            int count = 0;

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while(true) {
                    int[] arr = {1, 2, 4};
                    count++;
                    /**
                     * 获取abc
                     */
                    ctx.collect(Row.of(";Module接入一网:abc", "源:'规则名称:192.168.2.2dgfsdihgius", "APT", 1.9994, 1.22, arr));
                    Thread.sleep(1000);

//                    ctx.collect(Row.of(";Module;接入;二网123", "目的:事件:Web Applications:23.2.级别4.2jkdbgiusd","IPS", 1.9994, 1.22,1.3));
//                    Thread.sleep(1000);
//
//                    ctx.collect(Row.of(";Module接入二;网;123", "事件::23.2.4.2级别jkdbgiusd","IPS", 1.9994, 1.22,1.3));
//                    Thread.sleep(1000);
//
//                    ctx.collect(Row.of("Modu;le接入;四网123", "Module 目的:23.2.4.2jkdbgiusd","WAF", 1.9994, 1.22,1.3));
//                    Thread.sleep(1000);
//                    ctx.collect(Row.of("Module;接入四网;123", "Module:目的:23.2.4.2jkdbgiusd","WAF", 1.9994, 1.22,1.3));
//                    Thread.sleep(1000);
//
//                    ctx.collect(Row.of("Modu;le接入四网123", "目的攻击类型aSkyEyea发生时间:dbgiusd","SkyEye", 1.9994, 1.22,1.3));
//                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {
            }
        });

        env.enableCheckpointing(1000);
        env.setStateBackend(new MemoryStateBackend());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Row> ds = rowDataStreamSource.map((MapFunction) (row -> row))
                .returns(Types.ROW(new String[]{"name", "SUMMARY", "COMPONENT", "num1", "num2", "num3"},

                        new TypeInformation[]{Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.PRIMITIVE_ARRAY(Types.INT())}));
        SingleOutputStreamOperator<Row> ds2 = ds.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return Long.MIN_VALUE - 1L;
            }
        });


        //Caused by: java.lang.RuntimeException: Rowtime timestamp is null. Please make sure that a proper TimestampAssigner is defined and the stream environment uses the EventTime time characteristic.

        Table table = tEnv.fromDataStream(ds2);
        tEnv.createTemporaryView("test", table);

        Table table1 = tEnv.sqlQuery("select num3 from  test");
//        Table table1 = tEnv.sqlQuery("select regexp_extract(name,'(^;)*(.*)',2) from  test");
        DataStream<Row> dataStream = tEnv.toAppendStream(table1, Row.class);
        dataStream.print().setParallelism(16);


//        tEnv.registerFunction("timeTrans", new timeTrans());

//        Table table11 = tEnv.sqlQuery("select regexp_extract(SUMMARY,'(源:|目的:)(\\d+\\.\\d+\\.\\d+\\.\\d)',2) as aaa from TEST" );
//        Table table11 = tEnv.sqlQuery("select  name,case when regexp_extract(name,('接入一网'),0) is not null then '内网' else '互联网' end   as aaa from TEST" );
//        Table table11 = tEnv.sqlQuery("select  COMPONENT,SUMMARY,case when COMPONENT = 'SkyEye' then regexp_extract(SUMMARY,'(攻击类型)(.*)(发生时间)',2)  else '' end   as aaa from TEST" );

        /*
        Table table11 = tEnv.sqlQuery("select " +
                "'' as DVC_T,\n" +
                "'' as DVC_ID,\n" +
                "'' as EVT_T,\n" +
                "'' as DVC_A,\n" +
                "'' as AGT_A,\n" +
                "'' as RECORDER,\n" +
                "'' as RECV_TIME,\n" +
                "'' as AGT_ID,\n" +
                "'' as SEVERITY,\n" +
                "'' as CAT1,\n" +
                "'' as CAT2,\n" +
                "'' as K_C,\n" +
                "'' as NAME,\n" +
                "0 as SEVERITY_ID,\n" +
                "0 as CAT1_ID,\n" +
                "0 as CAT2_ID,\n" +
                "0 as K_C_ID,\n" +
                "cast ('1' as int) as NAME_ID,\n" +
                "'' as S_SEC_DOM,\n" +
                "'' as D_SEC_DOM,\n" +
                "'' as DESC,\n" +
                "'' as E_ID,\n" +
                "'' as `RAW`,\n" +
                "'' as `TIME`,\n" +
                "array[''] as D_IP,\n" +
                "array[''] as D_PORT,\n" +
                "'' as D_MAC,\n" +
                "array[''] as D_NATP,\n" +
                "array[''] as D_NATIP,\n" +
                "'' as D_CPE,\n" +
                "array[''] as D_ASSID,\n" +
                "'' as D_ASSN,\n" +
                "'' as D_ASSCAT,\n" +
                "'' as D_ASSOS,\n" +
                "'' as D_UNIT,\n" +
                "'' as D_UNTCAT,\n" +
                "array[''] as D_APPID,\n" +
                "'' as D_SCOPE,\n" +
                "'' as D_BCAT,\n" +
                "'' as D_ACOUNT,\n" +
                "array[''] as D_UID,\n" +
                "'' as D_UNAME,\n" +
                "'' as D_COUNTRY,\n" +
                "'' as D_PROVINCE,\n" +
                "'' as D_CITY,\n" +
                "'' as D_DISTRICT,\n" +
                "'' as D_ISP,\n" +
                "0 as D_LONG,\n" +
                "0 as D_LATI,\n" +
                "'' as D_DESC,\n" +
                "array[''] as S_IP,\n" +
                "array[''] as S_PORT,\n" +
                "'' as S_MAC,\n" +
                "array[''] as S_NATP,\n" +
                "array[''] as S_NATIP,\n" +
                "'' as S_CPE,\n" +
                "array[''] as S_ASSID,\n" +
                "'' as S_ASSN,\n" +
                "'' as S_ASSCAT,\n" +
                "'' as S_ASSOS,\n" +
                "'' as S_UNIT,\n" +
                "'' as S_UNITCAT,\n" +
                "array[''] as S_APPID,\n" +
                "'' as S_SCOPE,\n" +
                "'' as S_BCAT,\n" +
                "'' as S_ACOUNT,\n" +
                "array[''] as S_UID,\n" +
                "'' as S_UNAME,\n" +
                "'' as S_COUNTRY,\n" +
                "'' as S_PROVINCE,\n" +
                "'' as S_CITY,\n" +
                "'' as S_DISTRICT,\n" +
                "'' as S_ISP,\n" +
                "0 as S_LONG,\n" +
                "0 as S_LATI,\n" +
                "'' as S_DESC,\n" +
                "array[''] as CO_ID_LIST,\n" +
                "'' as ATTEN,\n" +
                "'' as MSG,\n" +
                "array[''] as S_IPV6,\n" +
                "array[''] as D_IPV6,\n" +
                "'' as CUSTOM1,\n" +
                "'' as CUSTOM2,\n" +
                "'' as CUSTOM3,\n" +
                "'' as CUSTOM4,\n" +
                "'' as CUSTOM5,\n" +
                "'' as DVC_V,\n" +
                "'' as VENDOR,\n" +
                "'' as SESSION_ID,\n" +
                "'' as PRO_T,\n" +
                "'' as PRO_A,\n" +
                "'' as FLE_P,\n" +
                "'' as FLE_N,\n" +
                "0 as FLE_S,\n" +
                "array[''] as TAGS,\n" +
                "'' as R_ID,\n" +
                "'' as R_NAME,\n" +
                "'' as REASON,\n" +
                "'' as SOLU,\n" +
                "'' as `RESULT`,\n" +
                "0 as `COUNT`,\n" +
                "0 as DUR,\n" +
                "'' as CON_T,\n" +
                "0 as PKG_T,\n" +
                "0 as PKG_A,\n" +
                "0 as BYTES_T,\n" +
                "0 as BYTES_A,\n" +
                "'' as URL,\n" +
                "'' as REQ_METHOD,\n" +
                "'' as REQ_AGENT,\n" +
                "'' as REFERER,\n" +
                "'' as RES_STATUS,\n" +
                "'' as REQ_UA,\n" +
                "'' as REQ_COOKIES,\n" +
                "'' as DOMAIN,\n" +
                "'' as ENCODE,\n" +
                "'' as CHARSET,\n" +
                "'' as REQ_BODY,\n" +
                "'' as RES_BODY,\n" +
                "'' as FLE_T,\n" +
                "'' as FLE_H,\n" +
                "'' as VUL,\n" +
                "'' as REQ_HEADER,\n" +
                "'' as RES_HEADER,\n" +
                "'' as PARAM,\n" +
                "'' as ATK_STATUS,\n" +
                "'' as ATK_T,\n" +
                "'' as ATK_DET,\n" +
                "'' as CUR_VALUE,\n" +
                "'' as CUR_TIME,\n" +
                "'' as ZONE_N,\n" +
                "'' as UNIT,\n" +
                "'' as DEF_METHOD,\n" +
                "'' as S_IP_LIST,\n" +
                "'' as SERVICE,\n" +
                "'' as AV,\n" +
                "'' as HIT_T,\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "0 as THREATSCORE from TEST\n" +
                "\n" );


         */
//        Table table11 = tEnv.sqlQuery("select Array[name] as aaaa from TEST where '2020-12-30 23:00:23' > '2020-12-30' " );
//        Table select = table11.select("array(aaaa) as aa");


//        table11.printSchema();
//        select.printSchema();

//        DataStream<Row> dataStream = tEnv.toAppendStream(table11, Row.class);
//        dataStream.print();
//        ((SingleOutputStreamOperator<Row>) ds).setParallelism(10).print();
//        ((SingleOutputStreamOperator<Row>) ds).setParallelism(10);
//        d23.print();
        env.execute();
    }
}




