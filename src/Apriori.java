import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
	
	public static int SUPORTE_MINIMO = 10; // porcentagem
	public static int TRANSACOES = 0;
	
	public static HashMap<String, Set<String>> contagem = new HashMap<String, Set<String>>();

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JobConf contagem = new JobConf(Apriori.class);
		contagem.setJobName("contagem");

		contagem.setOutputKeyClass(Text.class);
		contagem.setOutputValueClass(Text.class);

		contagem.setMapperClass(MapContagem.class);
		contagem.setReducerClass(ReduceContagem.class);
		
		contagem.setInputFormat(TextInputFormat.class);
		contagem.setOutputFormat(TextOutputFormat.class);
		
		String userHome = System.getProperty( "user.home" );
        
        File saida = new File(userHome + "/ap/contagem");
        
        if (saida.exists()) {
        	FileUtils.deleteDirectory(saida);
        }

        FileInputFormat.setInputPaths(contagem, new Path(userHome + "/projects/apriori/T40I10D100K.dat"));
        FileOutputFormat.setOutputPath(contagem, new Path(userHome + "/ap/contagem"));
		
		Job job1 = new Job(contagem);
		
		JobControl control = new JobControl("jobcontrol");
        control.addJob(job1);
        
        JobConf comb2 = new JobConf(Apriori.class);
        comb2.setJobName("contagem");

        comb2.setOutputKeyClass(Text.class);
        comb2.setOutputValueClass(Text.class);

        comb2.setMapperClass(MapConjuntos.class);
        comb2.setReducerClass(ReduceConjuntos.class);
        
        comb2.setNumReduceTasks(5);
		
        comb2.setInputFormat(TextInputFormat.class);
        comb2.setOutputFormat(TextOutputFormat.class);
        
        File saida2 = new File(userHome + "/ap/comb");
        
        if (saida2.exists()) {
        	FileUtils.deleteDirectory(saida2);
        }

        FileInputFormat.setInputPaths(comb2, new Path(userHome + "/ap/contagem/part-00000"));
        FileOutputFormat.setOutputPath(comb2, new Path(userHome + "/ap/comb"));
		
		Job job2 = new Job(comb2);
		job2.addDependingJob(job1);
		
		control.addJob(job2);
		
		
		
		JobConf corte = new JobConf(Apriori.class);
		corte.setJobName("contagem");

		corte.setOutputKeyClass(Text.class);
		corte.setOutputValueClass(Text.class);

		corte.setMapperClass(MapCorte.class);
		corte.setReducerClass(ReduceCorte.class);
		
		corte.setInputFormat(TextInputFormat.class);
		corte.setOutputFormat(TextOutputFormat.class);
        
        File saida3 = new File(userHome + "/ap/corte");
        
        if (saida3.exists()) {
        	FileUtils.deleteDirectory(saida3);
        }

        FileInputFormat.setInputPaths(
    		corte, 
    		new Path(userHome + "/ap/comb/part-00000"),
    		new Path(userHome + "/ap/comb/part-00001"),
    		new Path(userHome + "/ap/comb/part-00002"),
    		new Path(userHome + "/ap/comb/part-00003"),
    		new Path(userHome + "/ap/comb/part-00004")
        );
        FileOutputFormat.setOutputPath(corte, new Path(userHome + "/ap/corte"));
		
		Job job3 = new Job(corte);
		job3.addDependingJob(job2);
		
		control.addJob(job3);
		
		
		JobConf fechamento = new JobConf(Apriori.class);
		fechamento.setJobName("fechamento");

		fechamento.setOutputKeyClass(Text.class);
		fechamento.setOutputValueClass(Text.class);

		fechamento.setMapperClass(MapFechamento.class);
		fechamento.setReducerClass(ReduceFechamento.class);
		
		fechamento.setInputFormat(TextInputFormat.class);
		fechamento.setOutputFormat(TextOutputFormat.class);
        
        File saida4 = new File(userHome + "/ap/fechamento");
        
        if (saida4.exists()) {
        	FileUtils.deleteDirectory(saida4);
        }

        FileInputFormat.setInputPaths(fechamento, new Path(userHome + "/ap/corte/part-00000"));
        FileOutputFormat.setOutputPath(fechamento, new Path(userHome + "/ap/fechamento"));
		
		Job job4 = new Job(fechamento);
		job4.addDependingJob(job3);
		
		control.addJob(job4);
		
		
		JobConf regras = new JobConf(Apriori.class);
		regras.setJobName("regras");

		regras.setOutputKeyClass(Text.class);
		regras.setOutputValueClass(Text.class);

		regras.setMapperClass(MapRegras.class);
		regras.setReducerClass(ReduceRegras.class);
		
		regras.setInputFormat(TextInputFormat.class);
		regras.setOutputFormat(TextOutputFormat.class);
        
        File saida5 = new File(userHome + "/ap/regras");
        
        if (saida5.exists()) {
        	FileUtils.deleteDirectory(saida5);
        }

        FileInputFormat.setInputPaths(regras, new Path(userHome + "/ap/fechamento/part-00000"));
        FileOutputFormat.setOutputPath(regras, new Path(userHome + "/ap/regras"));
		
		Job job5 = new Job(regras);
		job5.addDependingJob(job4);
		
		control.addJob(job5);
        
        control.run();
	}

}
