package AccOperator.functions;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.types.Row;


/**
 * @Author: dxs
 * @Date: 2019/9/18
 */
public class TimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Row> {


  public TimestampExtractor(Time maxOutOfOrderness) {
    super(maxOutOfOrderness);
  }

  @Override
  public long extractTimestamp(Row row) {
    return System.currentTimeMillis();
//    return (Long) row.getField(2);
  }
}
