import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PageRank extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Path out = new Path(args[1]);

        Job preProcessJob = Job.getInstance(conf, "pre-process");
        preProcessJob.setJarByClass(PageRank.class);
        preProcessJob.setMapperClass(PreprocessMapper.class);
        preProcessJob.setReducerClass(PreprocessReducer.class);
        preProcessJob.setOutputKeyClass(Text.class);
        preProcessJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(preProcessJob, new Path(args[0]));
        TextOutputFormat.setOutputPath(preProcessJob, new Path(out + "/preprocess"));

        boolean success = preProcessJob.waitForCompletion(true);

        if (success) {
            conf.set("key.value.separator.in.input.line","\t");

            Job refactorFileJob = Job.getInstance(conf, "refactor file");
            refactorFileJob.setJarByClass(PageRank.class);

            refactorFileJob.setMapperClass(RefactorfileMapper.class);
            refactorFileJob.setPartitionerClass(RefactorfilePartitioner.class);
            refactorFileJob.setReducerClass(RefactorfileReducer.class);

            refactorFileJob.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
            org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.addInputPath(refactorFileJob, new Path(out + "/preprocess"));

            refactorFileJob.setOutputKeyClass(Text.class);
            refactorFileJob.setOutputValueClass(Text.class);
            TextOutputFormat.setOutputPath(refactorFileJob, new Path(out + "/refactorfile"));

            success = refactorFileJob.waitForCompletion(true);
            if (success) {
                for (Counter counter : refactorFileJob.getCounters().getGroup(RefactorfileReducer.PAGERANK)) {
                    conf.set(counter.getDisplayName(), "" + counter.getValue());
                }
            }
        }

        if (success) {
            conf.set("key.value.separator.in.input.line","\t");

            Job InitGraphJob = Job.getInstance(conf, "init graph");
            InitGraphJob.setJarByClass(PageRank.class);

            InitGraphJob.setMapperClass(InitGraphMapper.class);

            InitGraphJob.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
            org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.addInputPath(InitGraphJob, new Path(out + "/refactorfile"));

            InitGraphJob.setOutputKeyClass(Text.class);
            InitGraphJob.setOutputValueClass(Text.class);
            TextOutputFormat.setOutputPath(InitGraphJob, new Path(out + "/initgraph"));

            success = InitGraphJob.waitForCompletion(true);
            if (success) {
                double dangling = Double.parseDouble(conf.get("dangling"));
                long page = Long.parseLong(conf.get("page"));
                double delta = dangling / page;
                conf.set("delta", "" + delta * 1E8);
            }
        }

        if (success) {
            int i = 0;
            while (i < 10 && success) {
                Job job = Job.getInstance(conf, "iterative process");

                job.setJarByClass(PageRank.class);
                job.setMapperClass(IterMapper.class);
                job.setReducerClass(IterReducer.class);

                Path input = new Path(i == 0 ? out + "/initgraph" : out + "/iterativeProcess/output" + i);
                Path output = new Path(out + "/iterativeProcess/output" + (i + 1) );

                job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
                org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.addInputPath(job, input);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                TextOutputFormat.setOutputPath(job, output);

                success = job.waitForCompletion(true);

                if (success) {
                    for (Counter counter : job.getCounters().getGroup(IterMapper.ITER)) {
                        if (counter.getDisplayName().equals("delta")) {
                            conf.set("delta", "" + counter.getValue());
                        }
                    }
                }
                i++;
            }

        }

        if (success) {
            Job job = Job.getInstance(conf, "rank page");

            job.setJarByClass(PageRank.class);
            job.setMapperClass(RankMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setPartitionerClass(RankPartitioner.class);
            job.setSortComparatorClass(RankComparator.class);
            job.setReducerClass(RankReducer.class);

            Path input = new Path(out + "/iterativeProcess/output10");
            Path output = new Path(out + "/rankresult");

            job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
            org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.addInputPath(job, input);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            TextOutputFormat.setOutputPath(job, output);

            success = job.waitForCompletion(true);

        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new PageRank(), args);

        System.exit(status);

    }
}
