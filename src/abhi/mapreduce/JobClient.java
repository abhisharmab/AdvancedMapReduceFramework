/**
 * 
 */
package abhi.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.*;

/**
 * @author abhisheksharma
 *
 *
 * JobClient is the primary interface for the user-job to interact with the JobTracker. 
 * JobClient provides facilities to submit jobs, track their progress, access component-tasks' 
 * reports/logs, get the Map-Reduce cluster status information etc.
 * The users uses this to submit commands and job requests each for storing a file or running a map-reduce job

 * The job submission process involves: Main Tasks to be Done
 * Checking the input and output specifications of the job.
 * Ask the DataNodeLocal Manager to split up the file if NOT already spilt. 
 * Copying the job's jar and configuration to the map-reduce system directory on the distributed file-system.
 * Submitting the job to the JobTracker and optionally (maybe) monitoring it's status.
 * 
 * 
 * This class is in charge of interact with users, like submitting jobs, kill jobs and lookup
 * status, etc.
 */
public class JobClient implements IHandleClientRequest {

	public static void main(String[] args)
	{
		//Accept the commands from the user 
		//
	}

	@Override
	public void submitJob(JobConf jobConf) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean monitorandPrintJobInfo(JobConf jobConf, IRunningJobInfo job) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}
	
}