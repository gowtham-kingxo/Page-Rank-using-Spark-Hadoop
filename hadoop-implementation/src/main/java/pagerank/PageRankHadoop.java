package pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import twitter.followers.ReduceSideJoinFollowerTriangles;

import java.util.ArrayList;
import java.util.Arrays;

public class PageRankHadoop extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PageRankHadoop.class);

    enum DANGLING_NODE_PROBABILITY_DISTRIBUTION {
        DANGLING_NODE_PROBABILITY_ACCUMULATOR;
    }

    private class Vertex {
        private double pageRankValue;
        private String id;
        private ArrayList<String> adjacencyList;

        // Assigns the values from the serialized string
        Vertex (String serializedValue) {
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
                stringBuilder.append(":");
            }

            stringBuilder.append(this.pageRankValue);
            return stringBuilder.toString(); // vertex 1 connected to 2 and 3 with PR value 0.5 -> 1:2,3:0.3
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
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
