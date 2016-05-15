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
        		
        		try {
					context.write(key, new Text(StringUtils.join(transacoes.toArray(), " ")));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
        		Apriori.contagem.put(key.toString(), new HashSet<String>(transacoes));
        	}
        }
    }