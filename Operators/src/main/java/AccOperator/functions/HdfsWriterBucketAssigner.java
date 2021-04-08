package AccOperator.functions;

import akka.remote.serialization.StringSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.types.Row;

import java.util.UUID;

/**
 * @author ：zz
 */
public class HdfsWriterBucketAssigner implements BucketAssigner<Row, String> {

    @Override
    public String getBucketId(Row element, Context context) {
        String uuid = UUID.randomUUID().toString().replaceAll("-","");
        return "";
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
