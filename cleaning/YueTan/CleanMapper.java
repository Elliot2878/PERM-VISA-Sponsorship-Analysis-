
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String[] ParsedLine = value.toString().split(",");
        String line = value.toString();
        String[] cols=line.split(",");
        Text res = new Text();
        
        res.set(cols[2] + "," + cols[44] + "," + cols[45] + "," + cols[46].replaceAll("\\$", "") + "," + cols[128] + "," + cols[131]);
        context.write(NullWritable.get(), res);
    }


}
