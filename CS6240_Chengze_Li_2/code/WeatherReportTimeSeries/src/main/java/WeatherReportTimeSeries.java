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

public class WeatherReportTimeSeries {
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, StationYearPair, Text> {

        StationYearPair pair = new StationYearPair();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String stationId = itr.nextToken();
            String yearText = itr.nextToken().substring(0, 4);
            int year = Integer.parseInt(yearText);
            String recordType = itr.nextToken();
            String recordValue = itr.nextToken();

            if (recordType.equals("TMAX") || recordType.equals("TMIN")) {
                pair.setStationId(stationId);
                pair.setYear(year);
                context.write(pair, new Text(recordType + "_" + recordValue + "_" + year));
            }
        }
    }

    public static class MeanTempReducer
            extends Reducer<StationYearPair,Text,Text,Text> {

        public void reduce(StationYearPair key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String curYear = "" + key.getYear();

            int curMinTotal= 0;
            int curMinNums = 0;
            int curMaxTotal = 0;
            int curMaxNums = 0;

            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (Text val : values) {
                String convertedVal = val.toString();
                String[] record = convertedVal.split("_");

                if (record[2].equals(curYear)) { // if current record's year is as same as its previous one
                    if (record[0].equals("TMAX")) {
                        curMaxNums++;
                        curMaxTotal += Integer.parseInt(record[1]);
                    } else if (record[0].equals("TMIN")) {
                        curMinNums++;
                        curMinTotal += Integer.parseInt(record[1]);
                    } else {
                        continue;
                    }
                } else {  // current record is a new year's record
                    double maxMean = (double)curMaxTotal / curMaxNums;
                    double minMean = (double)curMinTotal / curMinNums;
                    sb.append("(" + curYear + ", " + "MaxMean: " + maxMean + ", " + "MinMean: " + minMean + "), ");

                    if (record[0].equals("TMAX")) {
                        curMaxNums = 1;
                        curMaxTotal = Integer.parseInt(record[1]);
                        curMinNums = 0;
                        curMinTotal = 0;
                    } else if (record[0].equals("TMIN")) {
                        curMinNums = 1;
                        curMinTotal = Integer.parseInt(record[1]);
                        curMaxNums = 0;
                        curMaxTotal = 0;
                    }
                    curYear = record[2];
                }
            }
            double maxMean = (double)curMaxTotal / curMaxNums;
            double minMean = (double)curMinTotal / curMinNums;
            sb.append("(" + curYear + ", " + "MaxMean: " + maxMean + ", " + "MinMean: " + minMean + ")]");
            context.write(key.getStationId(), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather report time series");
        job.setJarByClass(WeatherReportTimeSeries.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setPartitionerClass(StationYearPartitioner.class);
        job.setGroupingComparatorClass(StationYearGroupingComparator.class);
        job.setReducerClass(MeanTempReducer.class);
        job.setMapOutputKeyClass(StationYearPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
