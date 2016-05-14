import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Sets;

public class MapRegras extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
        	String line = value.toString();
            String[] values = line.trim().split("\t");
    
            String[] conjunto = values[0].split(",");
    		Set<String> subconjuntos = new HashSet<String>(Arrays.asList(conjunto));
        	Set<Set<String>> combinacoes = Sets.powerSet(subconjuntos);
        	
        	for (Iterator<Set<String>> iterator = combinacoes.iterator(); iterator.hasNext();) {
        		Set<String> o = iterator.next();
        		if (o.size() > 0 && o.size() < conjunto.length) {
        			List<String> direito = new ArrayList<String>();
        			
        			for (int i = 0; i < conjunto.length; i++) {
        				if (!o.contains(conjunto[i])) {
        					direito.add(conjunto[i]);
        				}
        			}
        			
        			output.collect(new Text(StringUtils.join(o.toArray(), ",") + " -> " + StringUtils.join(direito.toArray(), ",")), new Text(values[1]));
        		}
        	}
        }
    }