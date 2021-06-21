package flinkEnvironment;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author ：zz
 */
public class FlinkEnvironment {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private DataStream<Row> outputStream;
    private Table table;
    private DataStream<Row> element;

    public Table getTable() {
        return table;
    }

    public DataStream<Row> getElement() {
        return element;
    }

    public FlinkEnvironment setElement(DataStream<Row> element) {
        this.element = element;
        return this;
    }

    public FlinkEnvironment setTable(Table table) {
        this.table = table;
        return this;
    }


    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public FlinkEnvironment setEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public StreamTableEnvironment gettEnv() {
        return tEnv;
    }

    public FlinkEnvironment settEnv(StreamTableEnvironment tEnv) {
        this.tEnv = tEnv;
        return this;
    }

    public DataStream<Row> getOutputStream() {
        return outputStream;
    }

    public FlinkEnvironment setOutputStream(DataStream<Row> outputStream) {
        this.outputStream = outputStream;
        return this;
    }
}
