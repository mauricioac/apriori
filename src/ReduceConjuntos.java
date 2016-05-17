import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 public class ReduceConjuntos extends Reducer<Text, Text, Text, Text> {
		 
        public void reduce(Text key, Iterator<Text> values, Context context)
                throws IOException, InterruptedException {
        	
        	int totalLinhas = 0;
        	int contagemTotal = 0;
        	ArrayList<String> _linhas = new ArrayList<String>();
        	
        	while (values.hasNext()) {
        		String[] linha = values.next().toString().split("|");
        		int linhas = Integer.parseInt(linha[0]);
        		String[] conjLinhas = linha[1].split(",");
        		
        		totalLinhas += linhas;
        		contagemTotal += conjLinhas.length;
        		
        		for (int i = 0; i < conjLinhas.length; i++) {
        			_linhas.add(conjLinhas[i]);
        		}
        	}
        	
        	double sup = (100.0f * contagemTotal) / (double) totalLinhas;
        	
        	if (sup > Apriori.SUPORTE_MINIMO) {
        		context.write(key, new Text(StringUtils.join(_linhas.toArray())));
        	}
        }
    }