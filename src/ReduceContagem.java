import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 public class ReduceContagem extends Reducer<Text, Text, Text, Text> {
		 
        public void reduce(Text key, Iterator<Text> values, Context context)
                throws IOException, InterruptedException {
            
        	while (values.hasNext()) 
        	{
        		context.write(key, values.next());
        	}
        }
    }