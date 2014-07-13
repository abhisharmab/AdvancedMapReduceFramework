/**
 * 
 */
package abhi.mapreduce;

import java.io.IOException;

//MIGHT NOT NEED THIS FILE AT ALL> LET'S KEEP THINGS SIMPLE

/**
 * @author abhisheksharma
 *
 *This interface defines the User-Facing interface to get information about the running Map/Reduce
 *that the user submitted. 
 *
 *Client will get an object from running job from the JobClient on calling submitJob 
 *and then this Running will provide information such as the name of the job, configuration, and the 
 *primarily the current status of the Job.
 *
 */
public interface IRunningJobInfo {
	
	//Get the JobID
	public String getJobID();
	
	//Get the JobName
	public String getJobName();
	
	//Percentage Progress of the Map Tasks
	public float getMapProgress() throws IOException;
	
	//Percentage Progress of the Reduce Tasks
	public float getReduceProgress() throws IOException;
	
	//Is the Job Successfully Completed
	public boolean isSuccessful();

}
