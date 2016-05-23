import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.io.FileUtils;

public class Apriori {
	
	public static int SUPORTE_MINIMO = 3; // porcentagem
	public static int TRANSACOES = 0;
	
	public static HashMap<String, Set<String>> contagem = new HashMap<String, Set<String>>();

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String userHome = System.getProperty( "user.home" );
        
        File saida = new File(userHome + "/ap/contagem");
        
        if (saida.exists()) {
        	FileUtils.deleteDirectory(saida);
        }
		
		Job job1 = new Job();
		job1.setInputFormatClass(MultiLineInputFormat.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(MapContagem.class);
		job1.setReducerClass(ReduceContagem.class);
		
		FileInputFormat.setInputPaths(job1, new Path(userHome + "/apriori/T40I10D100K.dat"));
        FileOutputFormat.setOutputPath(job1, new Path(userHome + "/ap/contagem"));
        
        job1.submit();
        job1.waitForCompletion(true);
        
        File saida2 = new File(userHome + "/ap/conjuntos");
        
        if (saida2.exists()) {
        	FileUtils.deleteDirectory(saida2);
        }
		
		Job job2 = new Job();
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(MapConjuntos.class);
		job2.setReducerClass(ReduceConjuntos.class);
		
		FileInputFormat.setInputPaths(job2, new Path(userHome + "/ap/contagem/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(userHome + "/ap/conjuntos"));
		
        job2.waitForCompletion(true);
        
        File saida25 = new File(userHome + "/ap/fechamento");
        
        if (saida25.exists()) {
        	FileUtils.deleteDirectory(saida25);
        }
        
        Job fe = new Job();
        fe.setInputFormatClass(TextInputFormat.class);
        fe.setOutputKeyClass(Text.class);
        fe.setOutputValueClass(Text.class);
        fe.setMapperClass(MapFechamento.class);
        fe.setReducerClass(ReduceFechamento.class);
		
		FileInputFormat.setInputPaths(fe, new Path(userHome + "/ap/conjuntos/part-r-00000"));
        FileOutputFormat.setOutputPath(fe, new Path(userHome + "/ap/fechamento"));
		
        fe.waitForCompletion(true);
        
        File saida3 = new File(userHome + "/ap/regras");
        
        if (saida3.exists()) {
        	FileUtils.deleteDirectory(saida3);
        }
        
        Job job3 = new Job();
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapperClass(MapRegras.class);
        job3.setReducerClass(ReduceRegras.class);
		
		FileInputFormat.setInputPaths(job3, new Path(userHome + "/ap/fechamento/part-r-00000"));
        FileOutputFormat.setOutputPath(job3, new Path(userHome + "/ap/regras"));
		
        job3.submit();
	}

}
