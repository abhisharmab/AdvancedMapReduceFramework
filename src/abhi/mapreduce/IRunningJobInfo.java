/**
 * 
 */
package abhi.mapreduce;

import java.io.IOException;

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
	
	public String getJobID();
	
	public String getJobName();
	
	public float getMapProgress() throws IOException;
	
	public float getReduceProgress() throws IOException;
	
	public boolean isSuccessful();

}
