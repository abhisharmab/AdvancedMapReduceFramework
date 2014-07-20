/**
 * 
 */
package abhi.wordcount;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;

import abhi.mapreduce.JobClient;
import abhi.mapreduce.JobConf;

/**
 * @author abhisheksharma
 *
 */
public class WordCount implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WordCount(){

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if(args.length !=2)
		{
			System.out.println("Invalid Usage. Please pass the input and output path");
		}

		JobConf jobConf = new JobConf();
		jobConf.setJobName("WordCount");

		jobConf.setInputPath(args[0]);
		jobConf.setOutputPath(args[1]);

		jobConf.setInputFormatClassName("abhi.wordcount.WordCountInputFormat");
		jobConf.setPartitionerClassName("abhi.wordcount.WordCountPartitioner");
		jobConf.setOutputFormatClassName("abhi.wordcount.WordCountOutputFormat");

		jobConf.setMapperClassName("abhi.wordcount.WordCountMapper");
		jobConf.setReducerClassName("abhi.wordcount.WordCountReducer");

		jobConf.setReducerNum(1);

		JobClient jClient = new JobClient();
		try 
		{
			boolean result = jClient.submitJob(jobConf);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
