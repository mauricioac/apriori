import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

 public class ReduceConjuntos extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
		 
        @Override
        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            
        	
        	while (values.hasNext()) 
        	{
        		values.next();
        		count++;
        	}
        	
        	if (porcentagem > Apriori.SUPORTE_MINIMO) {
        		output.collect(key, new IntWritable(count));
        	}
        }
    }