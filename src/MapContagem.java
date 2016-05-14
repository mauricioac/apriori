import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapContagem extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
        	String line = value.toString();
            String[] values = line.trim().split(" ");
            Apriori.TRANSACOES += 1;
            
            for (int i = 0; i < values.length; i++) {
            	output.collect(new Text(values[i]), new Text(Apriori.TRANSACOES + ""));
            }
        }
    }