import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IterMapper
        extends Mapper<Text, Text, Text, Text> {
    public static final String ITER = "iter";

    public void setup(Context context) {
        // initialize the counter
        context.getCounter(ITER, "delta").setValue(0);
    }
    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] valueSplit = valueString.split("\\|"); // split the value to weight + list
        double weight = Double.parseDouble(valueSplit[0]);
        String valueData = valueSplit[1].substring(1, valueSplit[1].length() - 1); // remove []
        if (valueData.isEmpty()) {
            context.write(key, new Text("[]"));
        } else {
            String[] linkPages = valueData.split(", ");
            long outlinks = linkPages.length;

            for (String linkPage: linkPages) {
                context.write(new Text(linkPage), new Text(String.valueOf(weight / outlinks)));
            }
            context.write(key, new Text(valueSplit[1]));
        }

    }
}
