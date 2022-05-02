import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line  = value.toString();
        String[] arr = line.split(",");
        String occ_title = arr[9];
        String tot_emp = arr[11];
        String h_mean = arr[16];
        String a_mean = arr[17];
        String h_median = arr[21];
        String a_median = arr[26];
        String output = occ_title + ", " + tot_emp + ", " + h_mean + ", " + a_mean + ", " + h_median + ", " + a_median + ", ";

        context.write(new Text(output), new IntWritable(0));
    }
}
