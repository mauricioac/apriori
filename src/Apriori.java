import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.io.FileUtils;

public class Apriori {
	
	public static int SUPORTE_MINIMO = 15; // porcentagem
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
		
		FileInputFormat.setInputPaths(job1, new Path(userHome + "/projects/apriori/menor.dat"));
        FileOutputFormat.setOutputPath(job1, new Path(userHome + "/ap/contagem"));
		
		job1.submit();
	}

}
