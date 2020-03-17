package twitter.followers;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MapSideJoinFollowerTriangles extends Configured implements Tool {
    private static final int MAX_USER_ID = 1000000;
    private static final Logger logger = LogManager.getLogger(MapSideJoinFollowerTriangles.class);

    // Custom counters for the job
    public enum TRIANGLE_FOLLOWER {
        TOTAL_TRIANGLE_COUNT_WITH_DUPLICATES,
    }

    private static boolean isEdgeNodesLesserThanMaxValue(String[] edge) {
        if (edge.length < 2) {
            return false;
        }

        int followerIdValue = Integer.parseInt(edge[0]);
        int followeeIdValue = Integer.parseInt(edge[1]);
        return (followeeIdValue < MAX_USER_ID && followerIdValue < MAX_USER_ID);
    }

    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
        Text dummyKey = new Text("");
        Map<String, String> edgesCache = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] fileURIs = context.getCacheFiles();

            // Reads the input files and adds each edge to a Map, where key is the userId and
            // value is the list of all edges where the user is a follower, if the edge nodes values are lesser
            // than max user id value
            for (URI uri : fileURIs) {
                File file = new File(uri.getPath());
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                String line = bufferedReader.readLine();
                while (line != null) {
                    String[] edge = line.split(",");
                    if (MapSideJoinFollowerTriangles.isEdgeNodesLesserThanMaxValue(edge)) {
                        String val = edgesCache.getOrDefault(edge[0], "");
                        if (val.isEmpty()) {
                            val = edge[1];

                        } else {
                            val += "," + edge[1];
                        }

                        edgesCache.put(edge[0], val);
                    }

                    line = bufferedReader.readLine();
                }

                bufferedReader.close();
                fileReader.close();
            }
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split(",");
            String val = edgesCache.getOrDefault(input[1], "");

            if (val.isEmpty()) {
                return;
            }

            for (String node : val.split(",")) {
                context.write(dummyKey, new Text(node + "," + input[0]));
            }
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
        Text aggregate = new Text("aggregate");
        private final static IntWritable one = new IntWritable(1);
        Set<String> edgeCacheSet = new HashSet<>();
        private LongWritable trianglesCount = new LongWritable(0);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] fileURIs = context.getCacheFiles();
            // Reads the input files and adds each edge to a Set if the edge nodes values are lesser
            // than max user id value

            for (URI uri : fileURIs) {

                File file = new File(uri.getPath());
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                String line = bufferedReader.readLine();
                while (line != null) {
                    String[] edge = line.split(",");

                    if (MapSideJoinFollowerTriangles.isEdgeNodesLesserThanMaxValue(edge)) {
                        edgeCacheSet.add(line);
                    }

                    line = bufferedReader.readLine();
                }

                bufferedReader.close();
                fileReader.close();
            }
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            if (edgeCacheSet.contains(value.toString().trim())) {
                long newTrianglesCount = trianglesCount.get();
                newTrianglesCount += 1;

                this.trianglesCount.set(newTrianglesCount);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(TRIANGLE_FOLLOWER.TOTAL_TRIANGLE_COUNT_WITH_DUPLICATES).increment(trianglesCount.get());
        }
    }

    public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (final IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        JobControl jobControl = new JobControl("jobChain");

        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "Max-filter and individual user incoming-outgoing edges product");

        job1.setJarByClass(MapSideJoinFollowerTriangles.class);
        final Configuration jobConf = job1.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "");

        job1.setMapperClass(Mapper1.class);
        job1.setNumReduceTasks(0);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        List<java.nio.file.Path> paths = Files.walk(Paths.get(args[0]))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());

        for (java.nio.file.Path path : paths) {
            job1.addCacheFile(path.toUri());
        }

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);

        jobControl.addJob(controlledJob1);

        Configuration conf2 = getConf();
        final Job job2 = Job.getInstance(conf2, "Max-filter and total aggregation of incoming-outgoing " +
                "edges product");

        job2.setJarByClass(MapSideJoinFollowerTriangles.class);
        final Configuration jobConf2 = job2.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", ": ");

        job2.setMapperClass(Mapper2.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        for (java.nio.file.Path path : paths) {
            job2.addCacheFile(path.toUri());
        }

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
        // add the job to the job control
        jobControl.addJob(controlledJob2);
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            logger.log(Level.INFO, "Job running..");

            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                logger.log(Level.ERROR, "EXCCC: FROM CATCH!!");
            }
        }

        Counters cn = job2.getCounters();
        long totalNumberOfTriangles = cn.findCounter(TRIANGLE_FOLLOWER.TOTAL_TRIANGLE_COUNT_WITH_DUPLICATES).getValue() / 3;
        logger.log(Level.INFO, "Total number of triangles: " + totalNumberOfTriangles);

        System.exit(0);
        return (job1.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new MapSideJoinFollowerTriangles(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}