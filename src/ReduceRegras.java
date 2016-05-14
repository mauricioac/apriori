import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Sets;

 public class ReduceRegras extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {
		 
        @Override
        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
        	
        	String[] pedacos = key.toString().split(" -> ");
        	String[] esquerdo = pedacos[0].split(",");
        	String[] direito = pedacos[1].split(",");
        	
        	double conf = confianca(esquerdo, direito);
        	
        	output.collect(key, new Text("suporte: " + values.next().toString() + ", confianca: " + Double.toString(conf)));
        }
        
        public double confianca(String[] esquerdo, String[] direito)
        {
        	Set<String> intersecaoEsquerda = Apriori.contagem.get(esquerdo[0]);
        	
        	for (int i = 1; i < esquerdo.length; i++)
        	{
        		intersecaoEsquerda = Sets.intersection(intersecaoEsquerda, Apriori.contagem.get(esquerdo[i]));
        	}
        	
        	int contagemEsquerda = intersecaoEsquerda.size();
        	
        	for (int i = 0; i < direito.length; i++)
        	{
        		intersecaoEsquerda = Sets.intersection(intersecaoEsquerda, Apriori.contagem.get(direito[i]));
        	}
        	
        	int contagemDireita = intersecaoEsquerda.size();
        	
        	return (double) contagemDireita / (double) contagemEsquerda;
        }
    }