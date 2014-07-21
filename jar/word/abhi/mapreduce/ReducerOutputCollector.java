/**
 * 
 */
package abhi.mapreduce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author abhisheksharma
 * 
 * This Class collects the outputs from all the intermediate files and produces the final output
 * The numbers of parts of the files is dictated by the numberofReducers

 */
public class ReducerOutputCollector<KOUT, VOUT> extends OutputCollector<KOUT, VOUT> {

	//Using a BufferedWriter to Amortize the cost for Disk I/O
	//We want to fill up an buffer and then write out the data to the Disk
	private BufferedWriter bw;
	
	private OutputFormat<KOUT, VOUT> outFormat;
	
	private String outputFileName;
	
	public String getOutputFileName() {
		return outputFileName;
	}


	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}


	public ReducerOutputCollector(String outdirectory, String separator,
			OutputFormat<KOUT, VOUT> outFormat, int partitionedNum) {
		super(outdirectory, separator, "result");
		this.outFormat = outFormat;
		
		
		outputFileName = this.outputDirectory
                + System.getProperty("file.separator") + this.outputFileNamePrefix + "_part_" + partitionedNum ;
		try 
      	{
           //Add writer of each of the partition.
            setBw(new BufferedWriter(new FileWriter(outputFileName, true)));
        } 
      	catch (IOException e)
        {
          System.err.println("Output failed to create output file");
        }		
		
	}


	@Override
public void collect(KOUT key, VOUT value) throws IOException 
	{
		
		try
		{
			//TODO Abhi. We need to accept formatting from the user
			this.bw.write(this.outFormat.format(key, value));
		}
		catch (IOException e)
		{
	    	  System.err.println("Error writing to the output file");
	    	  e.printStackTrace();
		}
		
	}
	
	
	public void close() {
		    if (this.bw != null) {
		      try {
		        this.bw.flush();
		        this.bw.close();
		      } 
		      catch (IOException e) {
		    	  System.err.println("Error closing the output file");
		    	  e.printStackTrace();
		      }
		    }
		  }

	/**
	 * @return the bw
	 */
	public BufferedWriter getBw() {
		return bw;
	}

	/**
	 * @param bw the bw to set
	 */
	public void setBw(BufferedWriter bw) {
		this.bw = bw;
	}

}
