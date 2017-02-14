// map function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SenderCountMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {
@Override
    public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\\s");
			String IPaddr1 = new String();
			String IPaddr2 = new String();
			int last_dot;

            if (tokens[3].equals(">")) { // address #1 is sender
			    IPaddr1 = tokens[2];
                // eliminate the port part
                last_dot = IPaddr1.lastIndexOf('.');
                IPaddr1 = IPaddr1.substring(0, last_dot);
                // output the key, value pairs where the key is an
                // IP address 4-tuple and the value is 1 (count)
                context.write(new Text(IPaddr1), new IntWritable(1));
            }
            else { // address #2 is sender
                IPaddr2 = tokens[4];
                // eliminate the port part
                last_dot = IPaddr2.lastIndexOf('.');
                IPaddr2 = IPaddr2.substring(0, last_dot);
                // output the key, value pairs where the key is an
                // IP address 4-tuple and the value is 1 (count)
                context.write(new Text(IPaddr2), new IntWritable(1));
            }
		}
}

