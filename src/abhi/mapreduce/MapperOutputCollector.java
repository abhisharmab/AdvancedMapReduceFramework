/**
 * 
 */
package abhi.mapreduce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;



/**
 * @author abhisheksharma
 * 
 * This Class collects the outputs from all the Mappers and write to intermediate files. 
 * The numbers of parts of the files is dictated by the numberofReducers

 *
 */
public class MapperOutputCollector<KOUT, VOUT> extends OutputCollector<KOUT, VOUT> {

	private Partitioner<KOUT> paritioner; 
	
	//Writer that will spit out the intermediate files
	ArrayList<BufferedWriter> intermediateWriters;
	
	//Number of reducers chosen by the user
	private int numReducers;
	
	//Constructor
	public MapperOutputCollector(String outdirectory, Partitioner<KOUT> partitioner, int numReducers, String separator) 
	{
		super(outdirectory, separator);
		
		this.setParitioner(partitioner);
		this.intermediateWriters = new ArrayList<BufferedWriter>(); //Instance the ArrayList containing writers
		this.setNumReducers(numReducers);
		
		
		for(int i=0 ; i < this.numReducers ; i++)
		{
				try 
		      	{
		           //Add writer of each of the partition.
		           this.intermediateWriters.add(new BufferedWriter(new FileWriter(this.outputDirectory
		                  + System.getProperty("file.separator") + "part-" + i, true)));
		        } 
		      	catch (IOException e)
		        {
		          System.err.println("Output failed to create files");
		        }
		}
	}

	@Override
	protected void collect(KOUT key, VOUT value) throws IOException {
		// TODO Auto-generated method stub
		if(this.paritioner != null)
		{
			BufferedWriter bw = this.intermediateWriters.get(this.paritioner.getPartition(key, this.numReducers));
		    try 
		    {
		        bw.write(key + this.separator + value);
		        bw.newLine();
		        bw.flush();
		      } 
		    catch (IOException e) 
		    {
		    	System.err.println("Error writing the intermediate files");
		    }
		}
		else
		{ 
			//Fall back case is to write to just 1 file: At-least the job will still run even if the Applicaiton Programmer Screws Up
			System.err.println("partitioner was NULL. DEfault to single intermediate output file");
			BufferedWriter bw = this.intermediateWriters.get(1);
		    try 
		    {
		        bw.write(key + this.separator + value);
		        bw.newLine();
		        bw.flush();
		      } 
		    catch (IOException e) 
		    {
		        System.err.println("Error writing the intermediate files");
		        e.printStackTrace();
		    }
			
		}
	}
	
	
	  /**
	   * After the writes are done, close all files
	   */
	  public void closeAll() {
	    for (BufferedWriter bw : this.intermediateWriters) {
	      try 
	      {
	        bw.flush();
	        bw.close();
	      } catch (IOException e) 
	      {
	        System.err.println("Error closing the intermediate files");
	        e.printStackTrace();
	      }
	    }
	  }

	/**
	 * @return the paritioner
	 */
	public Partitioner<?> getParitioner() {
		return paritioner;
	}

	/**
	 * @param paritioner the paritioner to set
	 */
	public void setParitioner(Partitioner<KOUT> paritioner) {
		this.paritioner = paritioner;
	}

	/**
	 * @return the numReducers
	 */
	public int getNumReducers() {
		return numReducers;
	}

	/**
	 * @param numReducers the numReducers to set
	 */
	public void setNumReducers(int numReducers) {
		this.numReducers = numReducers;
	}

}
