import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Sets;

 public class ReduceRegras extends Reducer<Text, Text, Text, Text> {
		 
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	
        	String[] pedacos = key.toString().split(" -> ");
        	String[] esquerdo = pedacos[0].split(",");
        	String[] direito = pedacos[1].split(",");
        	
        	double conf[] = confianca(esquerdo, direito);
        	
        	context.write(key, new Text("suporte: " + conf[0] + ", confianca: " + Double.toString(conf[1])));
        }
        
        public double[] confianca(String[] esquerdo, String[] direito)
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
        	double conf = (double) contagemDireita / (double) contagemEsquerda;
        	
        	double[] r = {contagemDireita, conf};
        	
        	return r;
        }
    }