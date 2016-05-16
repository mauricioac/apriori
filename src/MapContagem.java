import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapContagem extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        	String val = value.toString();
            String[] linhas = val.trim().split("\n");
            
            List<Set<String>> itemsets = apriori(linhas);
        }
        
        public List<Set<String>> apriori(String[] linhas)
        {
        	ArrayList<ArrayList<String>> transacoes = new ArrayList<ArrayList<String>>();
        	HashMap<String, List<String>> ocorrencias = new HashMap<String, List<String>>();
        	
        	for (int i = 0; i < linhas.length; i++) {
        		String[] itens = linhas[i].trim().split(" ");
        		
        		transacoes.add(new ArrayList<String>(Arrays.asList(itens)));
        		
        		for (int j = 0; j < itens.length; j++) {
        			if (ocorrencias.get(itens[j]) == null) {
        				ocorrencias.put(itens[j], new ArrayList<String>());
        			}
        			
        			ocorrencias.get(itens[j]).add(Integer.toString(i));
        		}
        	}
        	
        	Set<String> keys = ocorrencias.keySet();
        	List<String> toRemove = new ArrayList<String>();
        	
        	for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
        		String k = iterator.next();
        		int count = ocorrencias.get(k).size();
        		
        		double suporte = (100.0f * count) / linhas.length;
        		
        		if (suporte <= Apriori.SUPORTE_MINIMO) {
        			toRemove.add(k);
        		}
        	}
        	
        	for (int i = 0; i < toRemove.size(); i++) {
        		ocorrencias.remove(toRemove.get(i));
        	}
        	
        	keys = ocorrencias.keySet();
        	
        	for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
        		String k = iterator.next();
        		System.out.println(k + " : " + ocorrencias.get(k));
        	}
        	
        	int k = 0;
        	boolean continuar = true;
        	
        	while (continuar) {
        		continuar = false;
        		
        		
        		
        		k++;
        	}
        	
        	return new ArrayList<Set<String>>();
        }
    }