import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * this class determines which records are grouped together for Reducer to process, in this case
 * all records with same stationId will be processed as a group
 */
public class StationYearGroupingComparator extends WritableComparator {
    public StationYearGroupingComparator() {
        super(StationYearPair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        StationYearPair pair1 = (StationYearPair)wc1;
        StationYearPair pair2 = (StationYearPair)wc2;

        return pair1.getStationId().compareTo(pair2.getStationId());
    }
}
