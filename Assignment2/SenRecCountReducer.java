// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class SenRecCountReducer
    extends Reducer<Text, CompositeWritable, Text, CompositeWritable> {

    private CompositeWritable result = new CompositeWritable();

    @Override
        public void reduce(Text key, Iterable<CompositeWritable> values, Context context)
        throws IOException, InterruptedException {

        long total_send = 0;
        long total_recv = 0;

        for (CompositeWritable value : values) {
            total_send += value.getSend();
            total_recv += value.getRecv();
        }

        result.setBytes(total_send, total_recv);

        context.write(key, result);
        }
}

