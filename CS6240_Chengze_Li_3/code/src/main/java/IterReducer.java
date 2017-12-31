import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IterReducer
        extends Reducer<Text,Text,Text,Text> {
    Configuration conf;
    public void setup(Context context) {
        conf = context.getConfiguration();
    }
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        double delta = Double.parseDouble(conf.get("delta")) / 1E8;
        double page = Double.parseDouble(conf.get("page"));
        double weight = 0;
        String list = "";
        for (Text value : values) {
            String item = value.toString();
            if (item.startsWith("[")) {  // the value represents adjacent list
                list = item;
            } else {
                Double cur = Double.parseDouble(item);
                weight += cur;
            }

        }

        weight += delta / page;
        if (list.equals("[]")) { // dangling nodes
            context.getCounter(IterMapper.ITER, "delta").increment((long)(weight * 1E8));
        }
        String newValue = "" + weight + "|" + list;
        context.write(key, new Text(newValue));
    }
}
