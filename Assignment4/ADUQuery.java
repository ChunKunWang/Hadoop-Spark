import scala.Tuple2;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;
import java.text.*;

public final class ADUQuery {
    private static SparkConf conf = new SparkConf().setAppName("ADUQuery");
    private static JavaSparkContext sc = new JavaSparkContext(conf);
    private static CommandLineParser parser = new BasicParser();
    private static HelpFormatter formatter = new HelpFormatter();
    private static Options options = new Options();
    private static String OutFile = "QueryResult";
    private static String TarIP = "";
    private static long RangeMax = -1;
    private static long RangeMin = -1;
    private static int Top = -1;
    private static int Show_Top = -1;

    public static void main(String[] args) throws Exception {

        boolean ExitConsole = false;
        if (args.length < 1) {
            System.err.println("Usage: ADUQuery <file>");
            System.exit(1);
        }

        // Create a new Spark Context
        JavaRDD<String> lines = sc.textFile(args[0]);
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
        CommandLine cmd = null;
        JavaRDD<String> s_words = null;
        JavaRDD<String> r_words = null;
        JavaPairRDD<String, Long> s_ones = null;
        JavaPairRDD<String, Long> r_ones = null;
        JavaPairRDD<String, Long> s_counts = null;
        JavaPairRDD<String, Long> r_counts = null;
        JavaPairRDD<String, Long> targets = null;

        System.out.println("Prepare Send/Recv data... ");
        s_words = ReadIPByte(lines, true); //sender->true, recver->false 
        s_ones = MapIPByte(s_words);
        s_counts = ReduceCount(s_ones);
        JavaPairRDD<String, Long> SendByteSorted = SortElements(s_counts, false); //false=descending, true=ascending

        r_words = ReadIPByte(lines, false); //sender->true, recver->false 
        r_ones = MapIPByte(r_words);
        r_counts = ReduceCount(r_ones);
        JavaPairRDD<String, Long> RecvByteSorted = SortElements(r_counts, false); //false=descending, true=ascending

        SetupOptions();
        while (!ExitConsole) {
            System.out.print("[ADUQuery]$ ");
            String Input = console.readLine();
            String delims = "[ ]+";
            String[] QueryArgs = Input.split(delims);

            if (QueryArgs[0].equals("send")) {
                System.out.println("Look at sender");
                cmd = ParseMyCLI(options, QueryArgs);
                targets = SendByteSorted; 

                if (cmd.hasOption("lt") || cmd.hasOption("gt"))
                    targets = FilterRange(targets, RangeMax, RangeMin);

                if (cmd.hasOption("sh")) ShowTopResult(targets, Show_Top);
                OutputResult(targets, OutFile, Top);
            }
            else if (QueryArgs[0].equals("recv")) {
                System.out.println("Look at receiver");
                cmd = ParseMyCLI(options, QueryArgs);
                targets = RecvByteSorted; 

                if (cmd.hasOption("lt") || cmd.hasOption("gt"))
                    targets = FilterRange(targets, RangeMax, RangeMin);

                if (cmd.hasOption("sh")) ShowTopResult(targets, Show_Top);

                OutputResult(targets, OutFile, Top);
            }
            else if (QueryArgs[0].equals("tar")) {
                if (QueryArgs.length == 2) {
                    System.out.println("Target the IP: "+QueryArgs[1]);

                    List<Long> L = SendByteSorted.lookup(QueryArgs[1]);
                    System.out.println("Total bytes sent: "+L);

                    L = RecvByteSorted.lookup(QueryArgs[1]);
                    System.out.println("Total bytes recv: "+L);
                }
                else System.out.println("IP needed: tar <IP>");
            }
            else if (QueryArgs[0].equals("tar-ext")) {
                if (QueryArgs.length == 2) {
                    System.out.println("Target the IP: "+QueryArgs[1]);

                    List<Long> L = SendByteSorted.lookup(QueryArgs[1]);
                    System.out.println("Total bytes sent: "+L);

                    List<Long> details = s_ones.lookup(QueryArgs[1]);
                    for (Long output : details) 
                        System.out.println("   |-> "+output);

                    L = RecvByteSorted.lookup(QueryArgs[1]);
                    System.out.println("Total bytes recv: "+L);

                    details = r_ones.lookup(QueryArgs[1]);
                    for (Long output : details) 
                        System.out.println("   |-> "+output);
                }
                else System.out.println("IP needed: tar-ext <IP>");
            }
            else if (QueryArgs[0].equals("help")) {
                help();
            } 
            else if (QueryArgs[0].equals("quit")) {
                System.out.println("Bye Bye~");
                ExitConsole = true;
            } else {
                LinuxCLI(Input);
            }

            cmd = null;
            ResetOptArg();
        }

        sc.stop();
    }

    private static JavaPairRDD<String, Long> SortElements(JavaPairRDD<String, Long> counts, 
            final boolean Ascending) {
        JavaPairRDD<Long, String> swaped = counts.mapToPair(
                new PairFunction<Tuple2<String, Long>, Long, String>() {
                    public Tuple2<Long, String> call(Tuple2<String, Long> tup) throws Exception {
                        return tup.swap();
                    }
                });

        JavaPairRDD<Long, String> sorted = swaped.sortByKey(Ascending);

        JavaPairRDD<String, Long> swap_sorted = sorted.mapToPair(
                new PairFunction<Tuple2<Long, String>, String, Long>() {
                    public Tuple2<String, Long> call(Tuple2<Long, String> tup) throws Exception {
                        return tup.swap();
                    }
                });

        return swap_sorted;
    }

    private static JavaPairRDD<String, Long> FilterRange(JavaPairRDD<String, Long> counts, 
            final Long max, final Long min) {
        JavaPairRDD<String, Long> filtered = counts.filter(
                new Function<Tuple2<String, Long>, Boolean>() {
                    @Override
            public Boolean call(Tuple2<String, Long> tup) {
                if (max == -1) return tup._2() >= min;
                else if (min == -1) return tup._2() <= max;
                else return tup._2() >= min && tup._2() <= max;
            }
            });
        return filtered;
    }

    private static JavaRDD<String> ReadIPByte(JavaRDD<String> lines, final boolean Sender) {
        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
            public Iterator<String> call(String s) {
                String[] tokens = s.split(" ");
                String IPaddr1 = new String();
                String IPaddr2 = new String();

                int last_dot;
                // get the two IP address.port fields
                IPaddr1 = tokens[2];
                IPaddr2 = tokens[4];

                if (Sender) { // Count for senders
                    if (tokens[3].equals(">")) { // address #1 is sender
                        last_dot = IPaddr1.lastIndexOf('.');
                        IPaddr1 = IPaddr1.substring(0, last_dot);
                        String[] hosts = {IPaddr1+" "+tokens[5]};
                        return Arrays.asList(hosts).iterator();
                    }
                    else {
                        last_dot = IPaddr2.lastIndexOf('.');
                        IPaddr2 = IPaddr2.substring(0, last_dot);
                        String[] hosts = {IPaddr2+" "+tokens[5]};
                        return Arrays.asList(hosts).iterator();
                    }
                }
                else { // Count for receivers
                    if (tokens[3].equals("<")) { // address #1 is receiver
                        last_dot = IPaddr1.lastIndexOf('.');
                        IPaddr1 = IPaddr1.substring(0, last_dot);
                        String[] hosts = {IPaddr1+" "+tokens[5]};
                        return Arrays.asList(hosts).iterator();
                    }
                    else {
                        last_dot = IPaddr2.lastIndexOf('.');
                        IPaddr2 = IPaddr2.substring(0, last_dot);
                        String[] hosts = {IPaddr2+" "+tokens[5]};
                        return Arrays.asList(hosts).iterator();
                    }

                }
            }
                }
        );
        return words;
    }

    private static JavaPairRDD<String, Long> MapIPByte(JavaRDD<String> words) {
        JavaPairRDD<String, Long> ones = words.mapToPair(
                new PairFunction<String, String, Long>() {
                    public Tuple2<String, Long> call(String s) {
                        String[] tokens = s.split(" ");
                        return new Tuple2<>(tokens[0], Long.parseLong(tokens[1],10)); //key = IP, value = 1
                    }
                });
        return ones;
    }


    private static JavaRDD<String> ReadIPCount(JavaRDD<String> lines, final boolean Sender) {
        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
            public Iterator<String> call(String s) {
                String[] tokens = s.split(" ");
                String IPaddr1 = new String();
                String IPaddr2 = new String();

                int last_dot;
                // get the two IP address.port fields
                IPaddr1 = tokens[2];
                IPaddr2 = tokens[4];

                if (Sender) { // Count for senders
                    if (tokens[3].equals(">")) { // address #1 is sender
                        last_dot = IPaddr1.lastIndexOf('.');
                        IPaddr1 = IPaddr1.substring(0, last_dot);
                        String[] hosts = {IPaddr1};
                        return Arrays.asList(hosts).iterator();
                    }
                    else {
                        last_dot = IPaddr2.lastIndexOf('.');
                        IPaddr2 = IPaddr2.substring(0, last_dot);
                        String[] hosts = {IPaddr2};
                        return Arrays.asList(hosts).iterator();
                    }
                }
                else { // Count for receivers
                    if (tokens[3].equals("<")) { // address #1 is receiver
                        last_dot = IPaddr1.lastIndexOf('.');
                        IPaddr1 = IPaddr1.substring(0, last_dot);
                        String[] hosts = {IPaddr1};
                        return Arrays.asList(hosts).iterator();
                    }
                    else {
                        last_dot = IPaddr2.lastIndexOf('.');
                        IPaddr2 = IPaddr2.substring(0, last_dot);
                        String[] hosts = {IPaddr2};
                        return Arrays.asList(hosts).iterator();
                    }

                }
            }
                }
        );
        return words;
    }

    private static JavaPairRDD<String, Long> MapIPCount(JavaRDD<String> words) {
        JavaPairRDD<String, Long> ones = words.mapToPair(
                new PairFunction<String, String, Long>() {
                    public Tuple2<String, Long> call(String s) {
                        return new Tuple2<>(s, 1L); //key = IP, value = 1
                    }
                });
        return ones;
    }

    private static JavaPairRDD<String, Long> ReduceCount(JavaPairRDD<String, Long> ones) {
        JavaPairRDD<String, Long> counts = ones.reduceByKey(
                new Function2<Long, Long, Long>() {
                    public Long call(Long i1, Long i2) {
                        return i1 + i2;
                    }
                });
        return counts;
    }

    public static void ShowTopResult(JavaPairRDD<String, Long> counts, Integer top) {
        List<Tuple2<String, Long>> output = counts.collect();

        if (top > 0) output = counts.take(top);
        if (top > 200) {
            System.out.println("ShowTopResult has max 200 lines.");
            output = counts.take(200);
        }
        for (Tuple2<?,?> tuple : output) 
            System.out.println(tuple._1() + ": " + tuple._2());
    }

    public static void OutputResult(JavaPairRDD<String, Long> counts, String filename, Integer top) {
        PrintStream original = new PrintStream(System.out);
        try {
            System.setOut(new PrintStream(new FileOutputStream(filename)));
            List<Tuple2<String, Long>> output = counts.collect();

            if (top > 0) { 
                output = counts.take(top);
            }

            for (Tuple2<?,?> tuple : output) {
                //the Tuple2 methods ._1() and ._2() get the pair 1st and 2nd values
                System.out.println(tuple._1() + ": " + tuple._2());
            }
        }
        catch (IOException e) {
            System.out.println("OutputResult error!");
        }
        System.setOut(original);
        System.out.println("Query output file: "+filename);
    }

    public static CommandLine ParseMyCLI(Options options, String[] QueryArgs) {
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, QueryArgs);
            if (cmd.hasOption("lt")) {
                //System.out.print("Option Max is present.  The value is: ");
                //System.out.println(cmd.getOptionValue("lt"));
                RangeMax = Long.parseLong(cmd.getOptionValue("lt"), 10);
            }
            if (cmd.hasOption("gt")) {
                //System.out.print("Option Min is present.  The value is: ");
                //System.out.println(cmd.getOptionValue("gt"));
                RangeMin = Long.parseLong(cmd.getOptionValue("gt"));
            }
            if (cmd.hasOption("top")) {
                //System.out.print("Option Top is present.  The value is: ");
                //System.out.println(cmd.getOptionValue("top"));
                Top = Integer.parseInt(cmd.getOptionValue("top"));
            }
            if (cmd.hasOption("sh")) {
                //System.out.print("Option Show is present.  The value is: ");
                //System.out.println(cmd.getOptionValue("sh"));
                Show_Top = Integer.parseInt(cmd.getOptionValue("sh"));
            }
            if (cmd.hasOption("o")) {
                //System.out.print("Option Output File is present.  The name is: ");
                //System.out.println(cmd.getOptionValue("o"));
                OutFile = cmd.getOptionValue("o");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            formatter.printHelp("send/recv", options);
        }

        return cmd;
    }

    public static void SetupOptions() {
        options.addOption("lt", "LessThan", true, "Setup Max Threshold");
        options.addOption("gt", "GreaterThan", true, "Setup Min Threshold");
        options.addOption("top", "Top", true, "Setup Top");
        options.addOption("sh", "Show", true, "Show Top # Results");
        options.addOption("o", "Outfile", true, "Setup Output File Name; default: QueryResult");
    }

    public static void LinuxCLI(String CLI) {
        if (CLI == null || CLI.isEmpty()) {
            help();
            return;
        }

        try {
            String s = null;
            Process p = Runtime.getRuntime().exec(CLI);
            BufferedReader stdInput = new BufferedReader(new 
                    InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new 
                    InputStreamReader(p.getErrorStream()));

            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
        }
        catch (IOException e) {
            System.out.println("Can't execute: " + CLI);
            help();
        }
    }

    public static void ResetOptArg() {
        RangeMax = -1;
        RangeMin = -1;
        Top = -1;
        Show_Top = -1;
        OutFile = "QueryResult";
    }

    public static void help() {
        System.out.println("Command: send, recv, tar, tar-ext, help, quit");
        formatter.printHelp("send/recv", options);
    }
}

