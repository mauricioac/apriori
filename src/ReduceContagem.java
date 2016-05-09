import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

 public class ReduceContagem extends MapReduceBase implements
            Reducer<Text, Text, Text, IntWritable> {
		 
        @Override
        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            
        	int count = 0;
        	
        	while (values.hasNext()) 
        	{
        		values.next();
        		count++;
        	}
        	
        	float porcentagem = (100.0f * count) / Apriori.TRANSACOES;
        	
        	if (porcentagem > Apriori.SUPORTE_MINIMO) {
        		output.collect(key, new IntWritable(count));
        	}
        }
    }