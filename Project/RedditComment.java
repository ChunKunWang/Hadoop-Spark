import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;
import java.text.*;

public final class RedditComment {
    private static int columns = 530; 
    private static int rows = 3;

    public static void main(String[] args) throws Exception {

        String OutFile = "RedditOut";
        if (args.length < 1) {
            System.err.println("Usage: RedditComment <file> <output>");
            System.exit(1);
        } else if (args.length == 2) {
            OutFile = args[1];
        }

        final String[][] MoralDic = BuildUpDic();

        SparkConf conf = new SparkConf().setAppName("RedCom");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
            public Iterator<String> call(String s) {
                String[] tokens = CleanRowData(s);

                if (tokens.length == 0) return Arrays.asList("Null").iterator();

                String[] keys = ExtractKey(tokens, MoralDic); 

                if ( keys.length == 0) return Arrays.asList("Null").iterator();

                //return Arrays.asList("Amos").iterator();
                return Arrays.asList(keys).iterator();
            }
            });


        JavaPairRDD<String, Long> ones = words.mapToPair(
                new PairFunction<String, String, Long>() {
                    public Tuple2<String, Long> call(String s) {
                        return new Tuple2<>(s, 1L); 
                    }
                });

        JavaPairRDD<String, Long> counts = ones.reduceByKey(
                new Function2<Long, Long, Long>() {
                    public Long call(Long i1, Long i2) {
                        return i1 + i2;
                    }
                });

        JavaPairRDD<String, Long> sorted = counts.sortByKey(true);

        System.setOut(new PrintStream(new FileOutputStream(OutFile)));

        //List<Tuple2<String, Long>> output = counts.collect();
        List<Tuple2<String, Long>> output = sorted.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        sc.stop();
    }

    public static String[] ExtractKey(String[] tokens, final String[][] MoralDic) {
        List<String> Keys = new ArrayList<>(); 

        //Keys.add(tokens[0]);
        for (int i = 0; i < tokens.length; i++) {
            //System.out.println(tokens[i]);
            for (int j = 0; j < columns; j++) {
                if (MoralDic[j][1] != null && !MoralDic[j][1].isEmpty()) {
                if (MoralDic[j][1].equals("1")) {
                    if(tokens[i].indexOf(MoralDic[j][0]) == 0 ) {
                        Keys.add(MoralDic[j][2]); 
                    }
                }
                else {
                    if (tokens[i].equals(MoralDic[j][0])) {
                        Keys.add(MoralDic[j][2]); 
                    }
                }
                }
                //else Keys.add("Amos["+j+"]");
            }
        }
        if (Keys.size() >= 1) Keys.add("Info");

        String[] ReturnKeys = new String[Keys.size()];
        ReturnKeys = Keys.toArray(ReturnKeys);

        return ReturnKeys;
    }

    public static String[] CleanRowData(String line) {
        String[] aArray = line.split("body\":\"");
        String[] bArray = aArray[1].split("\",\"");

        String Cline = bArray[0].replaceAll("[-+`.~@$#&%;=*^!':(),?]", "")
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

    public static String[][] BuildUpDic() {
        String[][] MoralDic = new String[columns][rows]; 

        System.out.print("Load 'MoralDic' and buildup dictorary: ");
        try {
            File file_dic = new File("MoralDic");
            FileReader fileDicReader = new FileReader(file_dic);
            BufferedReader bufferedDicReader = new BufferedReader(fileDicReader);
            String line;
            int i = 0;

            while ((line = bufferedDicReader.readLine()) != null) {
                String[] Array = line.split("/");
                MoralDic[i][0] = Array[0];
                MoralDic[i][1] = Array[1];
                MoralDic[i][2] = Array[2];
                //System.out.println(MoralDic[i][0]+"  "+MoralDic[i][1]+"  "+MoralDic[i][2]);
                i++;
            }
            fileDicReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done.");
        return MoralDic;
    }
}

