/**
 * 
 */
package abhi.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author abhisheksharma
 *
 * This interface defines the Services that the JobClient(running on each machine on the Cluster) 
 * must provide. 
 * 
 * These methods would define as the contract that the JobClient must adhere to in-order
 * to provide services to the User using that machine in the cluster
 * These are the set of necessary services that must be provided by the JobClient
 *
 */
public interface IHandleClientRequest {
	
	//Used to submit job to the JobClient which will further submit it to the Job Tracker
	public void submitJob() throws FileNotFoundException, IOException;
	
	//Reporting status of the Job on the user's command terminal
	public boolean monitorandPrintJobInfo() throws IOException, InterruptedException;
}
