import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

 public class ReduceCorte extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
		 
        @Override
        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
        	
        	int count = 0;
        	
        	while (values.hasNext()) {
        		values.next();
        		count++;
        	}
        	
        	float porcentagem = (100.0f * count) / Apriori.TRANSACOES;
        	
        	if (porcentagem > Apriori.SUPORTE_MINIMO) {
        		
        		output.collect(key, new Text(Integer.toString(count)));
        	}
        }
    }