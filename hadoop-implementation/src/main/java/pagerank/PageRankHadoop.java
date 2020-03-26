package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageRankHadoop extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PageRankHadoop.class);
    private static final int K = 3;
    private static final double ALPHA = 0.15;

    enum DANGLING_VERTICES_PROBABILITY_DISTRIBUTION {
        DANGLING_VERTICES_PROBABILITY_ACCUMULATOR;
    }

    private static class Vertex {
        private double pageRankValue;
        private String id;
        private List<String> adjacencyList;

        // Assigns the values from the serialized string
        Vertex(String serializedValue) {
            String[] parts = serializedValue.split(":");
            this.id = parts[0];
            this.pageRankValue = Double.parseDouble(parts[2]);

            if (parts[1].isEmpty()) {
                adjacencyList = null;
                return;

            }

            String[] neighborVertices = parts[1].split(",");
            adjacencyList = new ArrayList<>();
            adjacencyList.addAll(Arrays.asList(neighborVertices));
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(this.id);
            stringBuilder.append(":");

            if (this.adjacencyList != null && !adjacencyList.isEmpty()) {
                for (String vertexId : this.adjacencyList) {
                    stringBuilder.append(vertexId);
                    stringBuilder.append(",");
                }

                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            }

            stringBuilder.append(":");
            stringBuilder.append(this.pageRankValue);
            return stringBuilder.toString(); // vertex 1 connected to 2 and 3 with PR value 0.5 -> 1:2,3:0.3
        }
    }

    // Mapper that accumulates the probability distributed to dangling vertices
    public static class DanglingVerticesProbabilityAccumulatorMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Vertex vertex = new Vertex(value.toString());
            int vertexId = Integer.parseInt(vertex.id);

            boolean isDanglingVertex = (vertexId % K) == 0;
            if (isDanglingVertex) {
                // Since counter's type is long, multiple the page rank by max value to preserve precision
                context.getCounter(DANGLING_VERTICES_PROBABILITY_DISTRIBUTION.DANGLING_VERTICES_PROBABILITY_ACCUMULATOR)
                        .increment((long) (vertex.pageRankValue * Long.MAX_VALUE));
            }
        }
    }

    public static class PageRankCalculatorMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Vertex vertex = new Vertex(value.toString());
            // Emit vertex graph structure
            context.write(new Text(vertex.id), new Text(value));
            if (vertex.adjacencyList != null) {
                for (String neighborVertexId : vertex.adjacencyList) {
                    // value begins with 'val=' to differentiate vertex graph structure and page rank value
                    context.write(new Text(neighborVertexId), new Text("val=" + vertex.pageRankValue));
                }
            }
        }
    }

    public static class PageRankCalculatorReducer extends Reducer<Text, Text, Text, Text> {
        private static double probabilityFromDanglingNodesPerVertex;
        private static int totalVertices;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            double probabilityFromDanglingVertices =
                    Double.parseDouble(context.getConfiguration().get("PROBABILITY_FROM_DANGLING_VERTICES"));

            totalVertices = K * K;
            probabilityFromDanglingNodesPerVertex = probabilityFromDanglingVertices / totalVertices;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Vertex vertex = null;
            double pageRankSumFromIncomingEdges = 0.0;
            for (Text valueText : values) {
                String value = valueText.toString();
                if (value.startsWith("val=")) {
                    // page rank value
                    String pageRank = value.substring(4);
                    pageRankSumFromIncomingEdges += Double.parseDouble(pageRank);

                } else {
                    // vertex graph structure
                    vertex = new Vertex(value);
                }
            }

            if (vertex == null)
                return;

            double randomSurferProbability = ALPHA * (1.0 / totalVertices);
            double pageRankProbability = (1 - ALPHA) *
                    (probabilityFromDanglingNodesPerVertex + pageRankSumFromIncomingEdges);

            vertex.pageRankValue = randomSurferProbability + pageRankProbability;
            StringBuilder stringBuilder = new StringBuilder();
            if (vertex.adjacencyList != null && !vertex.adjacencyList.isEmpty()) {
                for (String vertexId : vertex.adjacencyList) {
                    stringBuilder.append(vertexId);
                    stringBuilder.append(",");
                }

                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            }

            context.write(new Text(vertex.id), new Text(stringBuilder.toString() + ":" + vertex.pageRankValue));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath;
        String outputPath;
        for (int i = 1; i <= 10; i++) {
            inputPath = (i % 2 == 1) ? args[0] : args[1];
            outputPath = (i % 2 == 1) ? args[1] : args[0];

            logger.info("Iteration:" + i);
            final Configuration conf = getConf();
            final Job job1 = Job.getInstance(conf, "Dangling vertices page rank accumulator");

            job1.setJarByClass(PageRankHadoop.class);

            final Configuration jobConf = job1.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", ":");

            job1.setJarByClass(PageRankHadoop.class);
            job1.setMapperClass(DanglingVerticesProbabilityAccumulatorMapper.class);

            //job.setCombinerClass(PRReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job1, new Path(inputPath));
            FileOutputFormat.setOutputPath(job1, new Path("dummy"));

            if (!job1.waitForCompletion(true))
                System.exit(1);

            Counters cn = job1.getCounters();
            long counterValue =
                    cn.findCounter(DANGLING_VERTICES_PROBABILITY_DISTRIBUTION.DANGLING_VERTICES_PROBABILITY_ACCUMULATOR).getValue();

            double totalPageRankFromDanglingVertices = (double) counterValue / Long.MAX_VALUE;
            conf.setDouble("PROBABILITY_FROM_DANGLING_VERTICES", totalPageRankFromDanglingVertices);
            logger.log(Level.INFO, "totalPageRankFromDanglingVertices: " + totalPageRankFromDanglingVertices);

            FileSystem fs = FileSystem.get(conf);

            fs.delete(new Path("dummy"), true);

            final Job job2 = Job.getInstance(conf, "Rage rank calculator");
            final Configuration jobConf2 = job2.getConfiguration();
            jobConf2.set("mapreduce.output.textoutputformat.separator", ":");

            System.out.println("Executing job 2");

            job2.setJarByClass(PageRankHadoop.class);
            job2.setMapperClass(PageRankCalculatorMapper.class);
            job2.setReducerClass(PageRankCalculatorReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(inputPath));
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            if (!job2.waitForCompletion(true))
                System.exit(1);

            fs.delete(new Path(inputPath), true);
        }

        return 0;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new PageRankHadoop(), args);
//            System.exit(0);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
