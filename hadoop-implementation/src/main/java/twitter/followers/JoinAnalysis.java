package twitter.followers;

import java.io.IOException;

//import org.apache.hadoop.util.
//import org.apache.commons.lang3.tuple.Pair;
//import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class JoinAnalysis extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(JoinAnalysis.class);

    public static class FolloweeMapper extends Mapper<Object, Text, Text, Text> {
        private static final int maxUserId = 250;

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // Mapper reads each line in the given document. The line has 2 values separated by comma.
            // The first value corresponds to the follower id and the second value represents the followee id
            String[] edge = value.toString().split(",");

            int followerValue = Integer.parseInt(edge[0]);
            int followeeValue = Integer.parseInt(edge[1]);
            if (followerValue < maxUserId && followeeValue < maxUserId) {
                Text follower = new Text(edge[0]);
                Text followee = new Text(edge[1]);
                context.write(follower, followee);
            }
//            word.set(followee);
            // The mapped key becomes the follower id and value is set as one since the mapper input corresponds
            // to a single edge.


//            context.write(word, one);

        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            // key corresponds to user id, list of values represents the count of followers mapped in the mapper stage.
            // Summing the list of values gives total follower count for a user who has at least one follower
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // 1.1 Map reduce classes for finding incoming and outgoing edges for each user
    public static class UserInputAndOutputEdgesCountMap extends Mapper<Object, Text, Text, Text> {
        private static final IntWritable one = new IntWritable(1);
        private final Text word = new Text("hi");
        private static final int maxUserId = 250;

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] edge = value.toString().split(",");
            Text follower = new Text(edge[0]);
            Text followee = new Text(edge[1]);

            int followerValue = Integer.parseInt(edge[0]);
            int followeeValue = Integer.parseInt(edge[1]);
            if (followerValue <= maxUserId && followeeValue <= maxUserId) {
                // For follower we are setting incoming edge zero and outgoing edge 1, vice versa for followee
                context.write(follower, new Text(0 + "," + 1));
                context.write(followee, new Text(1 + "," + 0));
            }
        }
    }

    // 1.2
    public static class UserInputAndOutputEdgesCountReduce extends Reducer<Text, Text, Text, Text> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int incomingEdgesSum = 0;
            int outgoingEdgesSum = 0;
            // key corresponds to user id, list of values represents the count of followers mapped in the mapper stage.
            // Summing the list of values gives total follower count for a user who has at least one follower
            for (Text value : values) {
                String[] pair = value.toString().split(",");
                incomingEdgesSum += Integer.parseInt(pair[0]);
                outgoingEdgesSum += Integer.parseInt(pair[1]);
            }

            int totalPath2IntermediateRecords = incomingEdgesSum * outgoingEdgesSum;
            Text output = new Text("" + totalPath2IntermediateRecords);
            context.write(key, output);
        }
    }


    // 2.1 Map class for aggregating product of incoming and outgoing edges for all users
    public static class UserInputAndOutputEdgesCountAggregateMap extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable count = new IntWritable(1);
        private final Text keyText = new Text("");

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] nodeProduct = value.toString().split(",");
            count.set(Integer.parseInt(nodeProduct[1]));
            keyText.set("aggregate");
            context.write(keyText, count);
        }
    }

    // 2.2 Reduce class for aggregating product of incoming and outgoing edges for all users
    public static class UserInputAndOutputEdgesCountAggregate extends Reducer<Text, IntWritable, Text, LongWritable> {
        private final LongWritable result = new LongWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            long totalAggregate = 0;
            // key corresponds to user id, list of values represents the count of followers mapped in the mapper stage.
            // Summing the list of values gives total follower count for a user who has at least one follower
            for (IntWritable value : values) {
                totalAggregate += value.get();
            }

            result.set(totalAggregate);
            context.write(key, result);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        JobControl jobControl = new JobControl("jobChain");

        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "Max-filter and individual user incoming-outgoing edges product");

        job1.setJarByClass(JoinAnalysis.class);
        final Configuration jobConf = job1.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS.
        job1.setMapperClass(UserInputAndOutputEdgesCountMap.class);
        // In mapper combiner is used in order to make the reduction process efficient.
        // The same IntSumCombiner
//        job.setCombinerClass(UserInputAndOutputEdgesCountReduce.class);
        job1.setReducerClass(UserInputAndOutputEdgesCountReduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);

        jobControl.addJob(controlledJob1);

        Configuration conf2 = getConf();
        final Job job2 = Job.getInstance(conf2, "Max-filter and total aggregation of incoming-outgoing " +
                "edges product");

        job2.setJarByClass(JoinAnalysis.class);
        final Configuration jobConf2 = job2.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", ": ");

        job2.setMapperClass(UserInputAndOutputEdgesCountAggregateMap.class);
        job2.setReducerClass(UserInputAndOutputEdgesCountAggregate.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
        // add the job to the job control
        jobControl.addJob(controlledJob2);
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                System.out.println("EXCCC: FROM CATCH!!");
            }
        }

        System.exit(0);
        return (job1.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new JoinAnalysis(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}