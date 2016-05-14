import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

 public class ReduceContagem extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
		 
        @Override
        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            
        	int count = 0;
        	List<String> transacoes = new ArrayList<String>();
        	
        	while (values.hasNext()) 
        	{
        		String valor = values.next().toString(); //número da linha
        		transacoes.add(valor);
        		count++;
        	}
        	
        	float porcentagem = (100.0f * count) / Apriori.TRANSACOES;
        	
        	if (porcentagem > Apriori.SUPORTE_MINIMO) {
        		
        		output.collect(key, new Text(StringUtils.join(transacoes.toArray(), " ")));
        		Apriori.contagem.put(key.toString(), new HashSet<String>(transacoes));
        	}
        }
    }