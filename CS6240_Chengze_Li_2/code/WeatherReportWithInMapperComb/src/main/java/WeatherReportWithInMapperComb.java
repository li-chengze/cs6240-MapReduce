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
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WeatherReportWithInMapperComb {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        Map<String, int[]> map;

        public void setup(Context context) {
            map = new HashMap<>();
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String stationId = itr.nextToken();
            itr.nextToken();
            String recordType = itr.nextToken();
            String recordValue = itr.nextToken();

            if (recordType.equals("TMAX") || recordType.equals("TMIN")) {
                if (!map.containsKey(stationId)) {
                    map.put(stationId, new int[4]); // map's value represent [maxValueTotal, maxValueNums, minValueTotal, minValueNums]
                }

                if (recordType.equals("TMAX")) {
                    int[] temp = map.get(stationId);
                    temp[0] += Integer.parseInt(recordValue);
                    temp[1]++;
                }

                if (recordType.equals("TMIN")) {
                    int[] temp = map.get(stationId);
                    temp[2] += Integer.parseInt(recordValue);
                    temp[3]++;
                }
            }
        }

        /**
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         * the output of mapper is ("stationId","maxTempTotal_maxTempNums_minTempTotal_minTempNums")
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, int[]> entry: map.entrySet()) {
                int[] temp = entry.getValue();
                context.write(new Text(entry.getKey()), new Text(temp[0] + "_" + temp[1] + "_" + temp[2] + "_" + temp[3]));
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

                maxTempTotal += Integer.parseInt(record[0]);
                maxTempNums += Integer.parseInt(record[1]);
                minTempTotal += Integer.parseInt(record[2]);
                minTempNums += Integer.parseInt(record[3]);
            }

            double meanMin = (double)minTempTotal / minTempNums;
            double meanMax = (double)maxTempTotal / maxTempNums;

            context.write(key, new Text("MeanMinTemp: " + meanMin + ", MeanMaxTemp: " + meanMax));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather report in mapper combine");
        job.setJarByClass(WeatherReportWithInMapperComb.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(MeanTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

