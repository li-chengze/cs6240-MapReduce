import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RankComparator extends WritableComparator {

    public RankComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        return -1 * w1.compareTo(w2);
    }
}
