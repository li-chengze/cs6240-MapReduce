import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RefactorfileReducer extends Reducer<Text,Text,Text,Text> {
    public static final String PAGERANK = "pagerank";
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        // get the total number of pages
        context.getCounter(PAGERANK, "page").increment(1);

        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Text value : values) {
            String item = value.toString();
            if (!item.equals("NULL")) {
                sb.append(value.toString() + ", ");
            }
        }
        String result = sb.toString();
        if (result.endsWith(", ")) {
            result = result.substring(0, result.length() - 2);
        }
        result += "]";
        if (result.equals("[]")) {
            context.getCounter(PAGERANK, "dangling").increment(1);
        }
        context.write(key, new Text(result));

    }

}
