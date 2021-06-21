package flinkTableApi.udfTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author ：zz
 */
public class demo implements TableSource<Row> {
    @Override
    public DataType getProducedDataType() {
        return null;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }

    @Override
    public String explainSource() {
        return null;
    }

}
