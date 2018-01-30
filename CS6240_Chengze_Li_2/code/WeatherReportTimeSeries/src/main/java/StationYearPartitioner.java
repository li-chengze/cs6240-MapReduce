import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * this class determines which record is sent to which reducer, from this case, the records
 * with same stationId will go to the same reducer
 */
public class StationYearPartitioner extends Partitioner<StationYearPair, Text> {

    @Override
    public int getPartition(StationYearPair pair, Text value, int numberOfReducer) {
        String stationId = pair.getStationId().toString();
        int sum = 0;

        for (char c : stationId.toCharArray()) {
            sum += c;
        }

        return sum % numberOfReducer;
    }
}
