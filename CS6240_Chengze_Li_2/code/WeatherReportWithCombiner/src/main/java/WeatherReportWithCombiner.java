import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WeatherReportWithCombiner {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String stationId = itr.nextToken();
            itr.nextToken();
            String recordType = itr.nextToken();
            String recordValue = itr.nextToken();

            if (recordType.equals("TMAX") || recordType.equals("TMIN")) {
                context.write(new Text(stationId), new Text(recordType + "_" + recordValue));
            }
        }
    }

    public static class MeanTempReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int minTempTotal = 0;
            int minTempNums = 0;
            int maxTempTotal = 0;
            int maxTempNums = 0;

            for (Text val : values) {
                String convertedVal = val.toString();
                String[] record = convertedVal.split("_");

                if (record[0].equals("TMAX")) {
                    maxTempNums++;
                    maxTempTotal += Integer.parseInt(record[1]);
                } else if (record[0].equals("TMIN")) {
                    minTempNums++;
                    minTempTotal += Integer.parseInt(record[1]);
                } else {
                    continue;
                }
            }

            double meanMin = (double)minTempTotal / minTempNums;
            double meanMax = (double)maxTempTotal / maxTempNums;

            context.write(key, new Text("MeanMinTemp: " + meanMin + ", MeanMaxTemp: " + meanMax));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather report with combiner");
        job.setJarByClass(WeatherReportWithCombiner.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(MeanTempReducer.class);
        job.setReducerClass(MeanTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


