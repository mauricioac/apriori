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
        	
        	Set<String> conjuntos = new HashSet<String>(codigos);
        	Set<Set<String>> combinacoes = Sets.powerSet(conjuntos);
        	
        	for (Iterator<Set<String>> iterator = combinacoes.iterator(); iterator.hasNext();) {
        		Set<String> o = iterator.next();
        		if (o.size() > 1) {
        			output.collect(new Text(StringUtils.join(o.toArray(), ",")), key);
        		}
        	}
        }
    }