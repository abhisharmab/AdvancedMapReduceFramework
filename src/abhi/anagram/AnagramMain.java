/**
 * 
 */
package abhi.anagram;

import java.io.IOException;
import java.io.Serializable;

import abhi.mapreduce.JobClient;
import abhi.mapreduce.JobConf;

/**
 * @author abhisheksharma
 *
 *
 * This is the main class for the Sample of Anagram.
 * We treat this as the application programmers code to test our application. 
 * 
 */
public class AnagramMain implements Serializable {

	private static final long serialVersionUID = 1L;

	public AnagramMain(){}

	public static void main(String[] args) {

		if(args.length !=2)
		{
			System.out.println("Invalid Usage. Please pass the input and output path");
		}

		JobConf jobConf = new JobConf();
		jobConf.setJobName("Anagram");

		jobConf.setInputPath(args[0]);
		jobConf.setOutputPath(args[1]);
		                                 
		jobConf.setInputFormatClassName("abhi.anagram.AnaInputFormat");
		jobConf.setPartitionerClassName("abhi.anagram.AnaPartitioner");
		jobConf.setOutputFormatClassName("abhi.anagram.AnaOutputFormat");

		jobConf.setMapperClassName("abhi.anagram.AnaMapper");
		jobConf.setReducerClassName("abhi.anagram.AnaReducer");

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
