import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
    extends Mapper<LongWritable, Text, Text, Text> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      boolean containNull = false;
      String line = value.toString();  
      String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
      // Selecting only the records from year 2016-2020
      if (values[0]!= "CountryID"){
         String country = values[0].replaceAll("[^0-9]","") + ","
                          + values[1].replaceAll("\\s+", "").toUpperCase();
         String data = "";
         int sum = 0;
         for (int i=48; i<values.length; i++){
             String entry = values[i].replaceAll("[^0-9]","");
             if (entry.equals("")){
                containNull = true;
                break;
             }
             data += ","+ entry;
             sum += Integer.parseInt(entry);
         }
        
         if (!containNull){
            double five_year_average = sum/5;
            context.write(new Text(""), new Text(country  + data + "," + five_year_average));
         }
      }  
      else{
         context.write(new Text(""), new Text(line + ",five_year_average"));
     }     

  }
}
