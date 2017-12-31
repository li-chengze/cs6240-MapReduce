import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InitGraphMapper extends Mapper<Text, Text, Text, Text> {
    Configuration conf;

    public void setup(Context context) {
        conf = context.getConfiguration();
    }
    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        long pageNumber = Long.parseLong(conf.get("page")); // get the number from driver

        double weight = 1.0 / pageNumber;

        String item = "" + weight + "|" + value.toString();

        context.write(key, new Text(item));
    }
}
