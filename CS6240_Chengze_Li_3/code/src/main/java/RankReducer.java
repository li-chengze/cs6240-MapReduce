import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {
    int count;
    public void setup(Context context) {
        count = 0;
    }

    public void reduce(DoubleWritable key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        for (Text value : values) {
            if (count == 100) {
                break;
            }
            count++;
            context.write(value, key);
        }
    }

}
