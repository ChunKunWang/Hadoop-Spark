import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class CompositeWritable implements Writable {
    private long val1 = 0;
    private long val2 = 0;

    public CompositeWritable() {}

    public CompositeWritable(long val1, long val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    public void setBytes(long val1, long val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    public long getSend() {
        return val1;
    }

    public long getRecv() {
        return val2;
    }

    @Override
        public void readFields(DataInput in) throws IOException {
            val1 = in.readLong();
            val2 = in.readLong();
        }

    @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(val1);
            out.writeLong(val2);
        }

    @Override
        public String toString() {
            return this.val1 + "\t" + this.val2;
        }
}


