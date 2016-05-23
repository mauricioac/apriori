import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Sets;

public class MapContagem extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        	String val = value.toString();
            String[] linhas = val.trim().split("\n");
            
            HashMap<Set<String>, ArrayList<String>> itemsets = apriori(linhas, context);
            
            Set<Set<String>> keys = itemsets.keySet();
        }
        
        public HashMap<Set<String>, ArrayList<String>> apriori(String[] linhas, Context context) throws IOException, InterruptedException
        {
        	ArrayList<ArrayList<String>> transacoes = new ArrayList<ArrayList<String>>();
        	HashMap<String, List<String>> ocorrencias = new HashMap<String, List<String>>();
        	
        	for (int i = 0; i < linhas.length; i++) {
        		String[] itens = linhas[i].trim().split(" ");
        		
        		transacoes.add(new ArrayList<String>(Arrays.asList(itens)));
        		
        		int l = Apriori.TRANSACOES;
        		
        		for (int j = 0; j < itens.length; j++) {
        			if (ocorrencias.get(itens[j]) == null) {
        				ocorrencias.put(itens[j], new ArrayList<String>());
        			}
        			
        			ocorrencias.get(itens[j]).add(Integer.toString(i));
        			if (Apriori.contagem.get(itens[j]) == null) {
        				Apriori.contagem.put(itens[j], new HashSet<String>());
        			}
        			Apriori.contagem.get(itens[j]).add(Integer.toString(l));
        		}
        		
        		Apriori.TRANSACOES += 1;
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
        	List<String> elementos = new ArrayList<String>();
        	
        	for (Iterator<String> iterator = keys.iterator(); iterator.hasNext();) {
        		String k = iterator.next();
        		elementos.add(k);
        	}
        	
        	int k = 2;
        	boolean continuar = true;
        	
        	HashMap<Set<String>, ArrayList<String>> finais = new HashMap<Set<String>, ArrayList<String>>();
        	
        	while (k < 4) {
        		continuar = false;
        		
        		List<List<String>> p = permutation(elementos, k);
        		
        		if (p.size() == 0) {
        			break;
        		}
        		
        		for (int i = 0; i < p.size(); i++) {
        			HashSet<String> set = new HashSet<String>(p.get(i));
        			Set<String> contagem = conta(set, ocorrencias);
        			double sup = (100.0f * contagem.size()) / (double) linhas.length;
        			
        			if (contagem.size() > 0) {
//        				continuar = true;
//        				finais.put(set, new ArrayList<String>(contagem));
        				context.write(new Text(StringUtils.join(set.toArray(), ",")), new Text(Integer.toString(linhas.length) + ";" + StringUtils.join(contagem.toArray(), ",") ));
        			}
        		}
        		
        		k++;
        	}
        	
        	return finais;
        }
        
        public List<List<String>> permutation(List<String> nums, int k) {
        	
            List<List<String>> conjuntos = new ArrayList<List<String>>();
            
            for (int i = 0; i < nums.size(); i++) {
            	ArrayList<String> temp = new ArrayList<String>();
            	temp.add(nums.get(i));
            	permuta(nums, conjuntos, temp, i, k, 1);
            }
            
            return conjuntos;
        }
        
        public void permuta(List<String> nums, List<List<String>> conjuntos, List<String> temp, int j, int k, int l)
        {
        	if (l < k) {
        		for (int i = j + 1; i < nums.size(); i++) {
            		List<String> temp2 = new ArrayList<String>(temp);
            		temp2.add(nums.get(i));
            		permuta(nums, conjuntos, temp2, i, k, l+1);
            	}
        	} else {
        		conjuntos.add(temp);
        	}
        }
        
        public Set<String> conta(Set<String> conjunto, HashMap<String, List<String>> ocorrencias)
        {
        	Set<String> intersecao = new HashSet<String>();
        	boolean primeiro = true;
        	
        	for (Iterator<String> iterator = conjunto.iterator(); iterator.hasNext();) {
        		String s = iterator.next();
        		
        		if (primeiro) {
        			intersecao = new HashSet<String>(ocorrencias.get(s));
        			primeiro = false;
        		} else {
        			intersecao = Sets.intersection(intersecao, new HashSet<String>(ocorrencias.get(s)));
        		}
        	}
        	
        	return intersecao;
        }
    }