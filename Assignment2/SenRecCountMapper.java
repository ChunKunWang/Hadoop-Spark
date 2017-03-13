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

public class SenRecCountMapper
    extends Mapper<LongWritable, Text, Text, CompositeWritable> {
    private CompositeWritable Obj_1 = new CompositeWritable();
    private CompositeWritable Obj_2 = new CompositeWritable();

    public static boolean containsOnlyNumbers(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i)))
                return false;
        }
        return true;
    }
    @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\s");
        String IPaddr1 = new String();
        String IPaddr2 = new String();
        String Output_1 = new String();
        String Output_2 = new String();
        int last_dot;
        Long IP_bytes;

        IPaddr1 = tokens[2];
        last_dot = IPaddr1.lastIndexOf('.');
        IPaddr1 = IPaddr1.substring(0, last_dot);

        IPaddr2 = tokens[4];
        last_dot = IPaddr2.lastIndexOf('.');
        IPaddr2 = IPaddr2.substring(0, last_dot);

        IP_bytes = Long.parseLong(tokens[5]);
        Obj_1.setBytes(IP_bytes, 0);
        //Obj_1.setBytes(1, 0);
        Obj_2.setBytes(0, IP_bytes);
        //Obj_2.setBytes(0, 1);

        if (containsOnlyNumbers(tokens[5])) {
            if (tokens[3].equals(">")) { // address #1 is sender
                context.write(new Text(IPaddr1), Obj_1);
                context.write(new Text(IPaddr2), Obj_2);
            }
            else { // address #2 is sender
                context.write(new Text(IPaddr2), Obj_1);
                context.write(new Text(IPaddr1), Obj_2);
            }
        }
        else {
            Obj_1.setBytes(1, 1);
            context.write(new Text("Amos"), Obj_1);
        }
    }
}

