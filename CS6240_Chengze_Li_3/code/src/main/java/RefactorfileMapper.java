import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RefactorfileMapper extends Mapper<Text, Text, Text, Text> {
    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        String values = value.toString();
        values = values.substring(1, values.length() - 1);
        String[] adjNodes = values.split(", ");

        for (String adjNode : adjNodes) {
            if (!adjNode.isEmpty()){
                context.write(key, new Text(adjNode));
                context.write(new Text(adjNode), new Text("NULL"));
            } else {
                context.write(key, new Text("NULL"));
            }
        }
    }
}
