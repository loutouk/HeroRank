import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Author: Louis Boursier
 * louisboursier@hotmail.fr
 *
 * PageRank Algorithm:
 * 1. Start each page at rank 1
 * 2. on each iteration, have page p contribute to rank(p) / |neighbors(p)| to its neighbors
 * 3. Set each page rank to 0.15 + 0.85 * contributions
 *
 */

public class Main {

    public static final boolean LOCAL_MODE = false;
    public static final int MAX_ITERATIONS = 100;
    public static JavaSparkContext sparkContext; // to avoid JavaSparkContext not serializable error

    public static void main(String[] args) {

        if (args.length != 4) {
            throw new IllegalArgumentException("Exactly 4 arguments are required: " +
                    "<inputPathUri> " +
                    "<outputPathUri> " +
                    "<numberOfIterations> " +
                    "<debugMode>");
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int iterations = Math.min(MAX_ITERATIONS, Integer.valueOf(args[2]));
        boolean debugMode = Boolean.valueOf(args[3]);

        JavaSparkContext sparkContext = null;
        if(LOCAL_MODE) {
            sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkTestJava").setMaster("local"));
        } else {
            sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkTestJava"));
        }

        JavaRDD<String> inputLines = null;

        for(int iteration=0 ; iteration<iterations ; iteration++) {

            // Load data from disk the first time, and then from RAM to reduce disk IO latency
            if(iteration == 0) { inputLines = sparkContext.textFile(inputPath); }

            // Saves neighbors for each key, will be used later
            JavaPairRDD<String, String> neighbors = inputLines.mapToPair(line -> {
                String[] lines = line.split("\\t");
                String key = lines[0];
                StringBuilder sb = new StringBuilder();
                String[] neighborsArray = Arrays.copyOfRange(lines, 2, lines.length);
                int i;
                for(i=0 ; i<neighborsArray.length-1 ; i++) { sb.append(neighborsArray[i] + "\t"); }
                if(i<lines.length) { sb.append(neighborsArray[i]); }
                return new Tuple2<>(key, sb.toString());
            });

            // Map
            JavaPairRDD<String, Double> flattenContributions = inputLines.flatMapToPair((String line) -> {
                String[] lines = line.split("\\t");
                String key = lines[0];
                Double rank = Double.valueOf(lines[1]);
                String[] neighborsArray = Arrays.copyOfRange(lines, 2, lines.length);
                ArrayList<Tuple2<String, Double>> contributions = new ArrayList<>();
                for(String neighbor : neighborsArray) {
                    contributions.add(new Tuple2<>(neighbor,(rank/(Double.valueOf(neighborsArray.length)))));
                }
                return contributions.iterator();
            });

            // Reduce
            JavaPairRDD<String, Double> aggregatedContributions =
                    flattenContributions.reduceByKey((aDouble, aDouble2) -> aDouble + aDouble2);

            // Compute rank according to the specified equation
            JavaPairRDD<String, Double> updatedContributions =
                    aggregatedContributions.mapValues(value -> value*0.85+0.15);

            // String cast because join needs to operate on similar data types
            JavaPairRDD<String, String> strUpdatedContributions = updatedContributions.mapValues(value ->
                    String.valueOf(value));

            JavaPairRDD<String, Tuple2<String, String>> join = strUpdatedContributions.join(neighbors);

            // Format it so that it corresponds to the file read at the beginning so that we can iterate over it again
            inputLines = join.map(stringTuple2Tuple2 ->
                    stringTuple2Tuple2._1 + "\t" +
                    stringTuple2Tuple2._2._1 + "\t" +
                    stringTuple2Tuple2._2._2);
        }
        if(debugMode) { inputLines.collect().forEach(s -> System.out.println(s)); }
        if(!LOCAL_MODE) { inputLines.saveAsTextFile(outputPath); }
    }

    // cb1e050d9f48e5cb734270d96678f089
}
