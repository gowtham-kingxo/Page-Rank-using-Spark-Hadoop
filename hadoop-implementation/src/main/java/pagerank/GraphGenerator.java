package pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class GraphGenerator {
    private static final int K = 1000;

    public static void main(String[] args) {
        String initialValue = "0.000001";

        FileWriter fw = null;
        try {
            fw = new FileWriter("input.txt", true);
            BufferedWriter bw = new BufferedWriter(fw);
            for (int i = 1; i <= K; i++) {
                int startVertexId = (i - 1) * K;
                int endVertexId = startVertexId + K;

                for (int j = startVertexId + 1; j < endVertexId; j++) {
                    String line = j + ":" + (j + 1) + ":" + initialValue + "\n";
                    bw.write(line);
                }

                String line = endVertexId + "::" + initialValue + "\n";
                bw.write(line);
            }

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
