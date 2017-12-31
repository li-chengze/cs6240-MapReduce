import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RankPartitioner extends Partitioner<DoubleWritable, Text> {
    @Override
    public int getPartition(DoubleWritable key, Text value, int numberOfReducer) {
        return 0;
    }
}
