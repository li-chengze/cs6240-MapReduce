import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PreprocessReducer
        extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(key, val);
        }
    }
}
