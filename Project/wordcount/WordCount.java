import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;
import java.text.*;

public final class WordCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: WordCount <file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
            public Iterator<String> call(String s) {
                String[] tokens = SplitRowData(s);

                return Arrays.asList(tokens).iterator();
            }
        });

        JavaRDD<String> filter = words.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                //System.out.println("Inside filter words ->" +s);
                if( s.trim().length() == 0) return false;
                return true;
            }
        });

        JavaPairRDD<String, Integer> ones = filter.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1); //key = IP, value = 1
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        JavaPairRDD<String, Integer> Sorted = SortElements(counts, false);

        System.setOut(new PrintStream(new FileOutputStream("wordcounts")));

        List<Tuple2<String, Integer>> output = Sorted.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        sc.stop();
    }

    private static JavaPairRDD<String, Integer> SortElements(JavaPairRDD<String, Integer> counts, 
            final boolean Ascending) {
        JavaPairRDD<Integer, String> swaped = counts.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
                        return tup.swap();
                    }
                });

        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(Ascending);

        JavaPairRDD<String, Integer> swap_sorted = sorted.mapToPair(
                new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tup) throws Exception {
                        return tup.swap();
                    }
                });

        return swap_sorted;
    }

    public static String[] SplitRowData(String line) {

        String Cline = line.replaceAll("[-+`.~@$#&%;=*^!':(),?]", "")
            .replaceAll("\\{", "")
            .replaceAll("\\}", "")
            .replaceAll("\\[", "")
            .replaceAll("\\]", "");

        Cline = Cline.replace("\\n", " ")
            .replace("_", " ")
            .replace("\\r", " ")
            .replace("\\", " ")
            .replace("/", " ")
            .replace("\"", " ");

        String[] subLine = Cline.toLowerCase().split(" ");

        return subLine;
    }

}

