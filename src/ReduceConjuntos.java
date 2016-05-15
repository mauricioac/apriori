import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

 public class ReduceConjuntos extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
		 
        @Override
        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
        	List<String> codigos = new ArrayList<String>();
        	
        	while (values.hasNext()) {
        		codigos.add(values.next().toString());
        	}
        	
        	Set<String> conjuntos = new TreeSet<String>(codigos);
        	
        	PowerSet<String> powerset = new PowerSet<String>(conjuntos);
        	System.out.println(key.toString());
        	System.out.println(codigos.toString());
        	
            for(Set<String> o:powerset)
            {
            	if (o.size() > 1 && suporte(o) > Apriori.SUPORTE_MINIMO) {
        			output.collect(new Text(StringUtils.join(o.toArray(), ",")), key);
        		}
            }
        }
        
        public double suporte(Set<String> conjunto)
        {
        	Set<String> intersecao = new HashSet<String>();
        	boolean primeiro = true;
        	
        	for (Iterator<String> iterator = conjunto.iterator(); iterator.hasNext();) {
        		String s = iterator.next();
        		
        		if (primeiro) {
        			intersecao = Apriori.contagem.get(s);
        			primeiro = false;
        		} else {
        			intersecao = Sets.intersection(intersecao, Apriori.contagem.get(s));
        		}
        	}
        	
        	return (100.0f * intersecao.size()) / Apriori.TRANSACOES;
        }
    }