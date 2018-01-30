import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankMapper
        extends Mapper<Text, Text, DoubleWritable, Text>{
    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        String item = value.toString();
        String[] split = item.split("\\|");
        Double d = Double.parseDouble(split[0]);
        context.write(new DoubleWritable(d), key);
    }
}
