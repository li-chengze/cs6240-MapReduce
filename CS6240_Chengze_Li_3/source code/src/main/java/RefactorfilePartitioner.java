import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RefactorfilePartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numberOfReducer) {
        return 0;
    }
}
