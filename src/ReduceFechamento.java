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
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Sets;

 public class ReduceFechamento extends Reducer<Text, Text, Text, Text> {
		 
        public void reduce(Text key, Iterator<Text> values, Context context)
                throws IOException, InterruptedException {
        	
        	List<HashSet<String>> conjuntos = new ArrayList<HashSet<String>>();
        	List<Integer> remover = new ArrayList<Integer>();
        	
        	while (values.hasNext()) {
        		String[] conj = values.next().toString().split(",");
        		conjuntos.add(new HashSet<String>(Arrays.asList(conj)));
        	}
        	
        	for (int i = 0; i < conjuntos.size(); i++) {
        		HashSet<String> a = conjuntos.get(i);
        		
        		for (int j = 0; j < conjuntos.size(); j++) {
        			if (i == j) {
        				continue;
        			}
        			
        			HashSet<String> b = conjuntos.get(j);
        			
        			if (isSubset(a, b)) {
        				remover.add(i);
        			}
        		}
        	}
        	
        	for (int i = 0; i < conjuntos.size(); i++) {
        		if (remover.contains(i)) {
        			continue;
        		}
        		
        		context.write(new Text(StringUtils.join(conjuntos.get(i).toArray(), ",")), key);
        	}
        }
        
        public boolean isSubset(Set<String> a, Set<String> b)
        {
        	boolean subconjunto = true;
        	
        	for (Iterator<String> iterator = a.iterator(); iterator.hasNext();) {
        		String o = iterator.next();
        		
        		if (!b.contains(o)) {
        			subconjunto = false;
        		}
        	}
        	
        	return subconjunto;
        }
    }