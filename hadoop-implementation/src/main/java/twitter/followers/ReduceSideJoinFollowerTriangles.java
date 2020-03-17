package twitter.followers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ReduceSideJoinFollowerTriangles extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ReduceSideJoinFollowerTriangles.class);
    private static final long MAX_USER_ID = 10000;

    // Custom counters for the job
    public enum TRIANGLE_FOLLOWER {
        TOTAL_TRIANGLE_COUNT_WITH_DUPLICATES,
    }

    private static boolean isEdgeNodesLesserThanMaxValue(String[] edge) {
        long followerIdValue = Long.parseLong(edge[0]);
        long followeeIdValue = Long.parseLong(edge[1]);
        return (followeeIdValue < MAX_USER_ID && followerIdValue < MAX_USER_ID);
    }

    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] edge = value.toString().split(",");
            // Filter input data based on max user id value
            if (ReduceSideJoinFollowerTriangles.isEdgeNodesLesserThanMaxValue(edge)) {
                Text followerKey = new Text(edge[0]);
                Text followeeKey = new Text(edge[1]);

                // Two emits for each input record
                // Tagging: A represents the that the key appears in the first position in edge and
                // B represents that the key appears in the second position in edge
                Text followerValue = new Text("A:" + edge[1]);
                Text followeeValue = new Text("B:" + edge[0]);

                context.write(followerKey, followerValue);
                context.write(followeeKey, followeeValue);
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        private static Text key = new Text("");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> followersList = new ArrayList<>();
            List<String> followingList = new ArrayList<>();

            for (Text value : values) {
                String[] valueParts = value.toString().split(":");
                String tag = valueParts[0];

                if (tag.equals("A")) {
                    followingList.add(valueParts[1]);

                } else {
                    followersList.add(valueParts[1]);
                }
            }

            for (String followerUserId : followersList) {
                for (String followingUserId : followingList) {
                    Text path2Value = new Text(followerUserId + "," + key + "," + followingUserId);
                    context.write(new Text(followingUserId), path2Value);
                }
            }
        }
    }

    /**
     * Mapper for parsing the path2 output of first job and emit
     */
    public static class Mapper2IntermediateData extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] intermediateDataParts = value.toString().split(":");
            Text endUserIdInPath2 = new Text(intermediateDataParts[0]);
            Text path2Triplet = new Text(intermediateDataParts[1]);
            context.write(endUserIdInPath2, path2Triplet);
        }
    }

    /**
     * Mapper for parsing the edges.csv and emit
     */
    public static class Mapper2Edges extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] edge = value.toString().split(",");

            if (ReduceSideJoinFollowerTriangles.isEdgeNodesLesserThanMaxValue(edge)) {
                Text followerUserId = new Text(edge[0]);
                context.write(followerUserId, value);
            }
        }
    }

    /**
     * Reducer for computing triangles from path2 data
     */
    public static class ReducerForComputingTriangle extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> path2Data = new ArrayList<>();
            Set<String> edges = new HashSet<>();

            for (Text value : values) {

                String[] parts = value.toString().split(",");
                if (parts.length == 3) {
                    path2Data.add(value.toString());

                } else {
                    edges.add(value.toString());
                }
            }

            for (String triplet : path2Data) {
                String[] parts = triplet.split(",");

                String missingEdge = parts[2] + "," + parts[0];
                if (edges.contains(missingEdge)) {
                    context.getCounter(TRIANGLE_FOLLOWER.TOTAL_TRIANGLE_COUNT_WITH_DUPLICATES).increment(1);
//                    context.write(new Text(), new Text(triplet));
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        JobControl jobControl = new JobControl("Reduce side join");

        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "Path2 Values generator");

        job1.setJarByClass(ReduceSideJoinFollowerTriangles.class);

        final Configuration jobConf = job1.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ":");
        // Delete output directory, only to ease local development; will not work on AWS.
        job1.setMapperClass(ReduceSideJoinFollowerTriangles.Mapper1.class);
        job1.setReducerClass(ReduceSideJoinFollowerTriangles.Reducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);

        jobControl.addJob(controlledJob1);

        final Configuration conf2 = getConf();
        final Job job2 = Job.getInstance(conf2, "Path2 Values generator");

        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class,
                ReduceSideJoinFollowerTriangles.Mapper2Edges.class);

        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class,
                ReduceSideJoinFollowerTriangles.Mapper2IntermediateData.class);

        job1.setJarByClass(ReduceSideJoinFollowerTriangles.class);

        final Configuration jobConf2 = job2.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", "");
        // Delete output directory, only to ease local development; will not work on AWS.
//        job1.setMapperClass(ReduceSideJoinFollowerTriangles.Mapper2IntermediateData.class);
        job2.setReducerClass(ReduceSideJoinFollowerTriangles.ReducerForComputingTriangle.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

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
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                logger.log(Level.ERROR, "EXCCC: FROM CATCH!!");
            }
        }

        Counters cn = job2.getCounters();
        long totalNumberOfTriangles = cn.findCounter(ReduceSideJoinFollowerTriangles.TRIANGLE_FOLLOWER.TOTAL_TRIANGLE_COUNT_WITH_DUPLICATES).getValue() / 3;
        logger.log(Level.INFO, "Total number of triangles: " + totalNumberOfTriangles);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ReduceSideJoinFollowerTriangles(), args);
            System.exit(0);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
