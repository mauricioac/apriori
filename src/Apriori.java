import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.commons.io.FileUtils;

public class Apriori {
	
	public static int SUPORTE_MINIMO = 5; // porcentagem
	public static int TRANSACOES = 0;

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JobConf contagem = new JobConf(Apriori.class);
		contagem.setJobName("contagem");

		contagem.setOutputKeyClass(Text.class);
		contagem.setOutputValueClass(IntWritable.class);

		contagem.setMapperClass(MapContagem.class);
		contagem.setReducerClass(ReduceContagem.class);
		
		contagem.setInputFormat(TextInputFormat.class);
		contagem.setOutputFormat(TextOutputFormat.class);
        
        File saida = new File("/home/mauricio/apriori/contagem");
        
        if (saida.exists()) {
        	FileUtils.deleteDirectory(saida);
        }

        FileInputFormat.setInputPaths(contagem, new Path("/home/mauricio/projects/apriori/menor.dat"));
        FileOutputFormat.setOutputPath(contagem, new Path("/home/mauricio/apriori/contagem"));
		
		Job job1 = new Job(contagem);
		
		JobControl control = new JobControl("jobcontrol");
        control.addJob(job1);
        
        control.run();
	}

}