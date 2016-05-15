import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapContagem extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
//        	String line = value.toString();
//            String[] values = line.trim().split(" ");
//            Apriori.TRANSACOES += 1;
//            
//            for (int i = 0; i < values.length; i++) {
//            	output.collect(new Text(values[i]), new Text(Apriori.TRANSACOES + ""));
//            }
        	
        	System.out.println(key.toString());
        	System.out.println(value.toString());
        	System.out.println("-----------");
        }
    }