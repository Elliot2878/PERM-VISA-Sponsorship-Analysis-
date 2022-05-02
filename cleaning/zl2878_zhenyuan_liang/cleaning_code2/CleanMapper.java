import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line  = value.toString();
        String[] arr = line.split(",");
	boolean hasNan = false;
        for (int i = 0; i < arr.length; i++) {
            if(arr[i].contains("*") || arr[i].contains("#")) {
		hasNan = true;
		break;
            }
        }

	if(hasNan == false) {
		context.write(new Text(value), NullWritable.get());
	}
    }
}
