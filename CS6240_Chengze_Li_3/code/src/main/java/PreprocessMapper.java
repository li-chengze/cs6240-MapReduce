import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PreprocessMapper
        extends Mapper<Object, Text, Text, Text> {
    Bz2WikiParser parser = new Bz2WikiParser();
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();
        String parsed = parser.process(line);
        if (!parsed.equals("unvalid")){
            String[] splitted = parsed.split("~");

            context.write(new Text(splitted[0]), new Text(splitted[1]));
        }

    }
}
