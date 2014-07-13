/**
 * 
 */
package abhi.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import abhi.adfs.NameNodeMaster;

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
public class JobClient implements IClientServices {

	//Remote Reference of the JobTracker Services to that we can Call Services upon it
	private IJobTrackerServices jobTrackerServiceProvider;
	private NameNodeMaster nameNodeMasterReference; 
	Registry rmiRegistry = null;
	
	public JobClient()
	{
		try
		{
			int registryPort = Integer.parseInt(SystemConstants.getConfig(SystemConstants.REGISTRY_PORT));
			rmiRegistry = LocateRegistry.getRegistry(SystemConstants.getConfig(SystemConstants.REGISTRY_HOST),registryPort);
			this.jobTrackerServiceProvider = (IJobTrackerServices) rmiRegistry.lookup(SystemConstants.getConfig(SystemConstants.JOBTRACKER_SERVICE_NAME));
		
		}
		catch(NumberFormatException | RemoteException | NotBoundException e)
		{
			System.err.println("Error occurred in communcating with JobTracker");
			System.err.println("Ensure Jobtracker is running and check configuration");
		}
		
		try
		{
			//Check with Douglas
			this.nameNodeMasterReference = (NameNodeMaster)rmiRegistry.lookup(SystemConstants.getConfig(SystemConstants.NAMENODE_SERVICE_NAME));
		
		}
		catch(NumberFormatException | RemoteException | NotBoundException e)
		{
			System.err.println("Error occurred in communcating with NameNode via the Registry");
		}

	}
	
	public static void main(String[] args)
	{
	}

	@Override
	public boolean submitJob(JobConf jobConf) throws FileNotFoundException, IOException {
		//1. Check if the Job Configuration is Valid
		if(jobConf == null || !IsJobConfValid(jobConf))
		{
			System.err.println("Invalid Job Configuration Submitted. Please check your Job Source Code and Config");
			return false;
		}
		
		//2. Talk to the
		//TODO: Abhi needs call upon 
		
		//Piggyback on this JobId to Report Progress for the Client about the Job that he request to Run
		int uniqueJobID = requestJobIDfromJobTracker();
		if(uniqueJobID <= 0){
		      System.err.println("The system is not available for submitting new job.");
	      return false;
	    } else {
	      jobConf.setJobID(uniqueJobID);
	    }
		
		if(jobConf.getJobName() == null || jobConf.getJobName().length() == 0)
			jobConf.setJobName(String.valueOf(uniqueJobID));
		
	    try {
	        if (this.jobTrackerServiceProvider.submitJob(jobConf)) 	    
	        {
	          System.out.println("JobClient submmited Job successfully.");
	          return true;
	        }
	        else
	        {
	          System.out.println("Failed to submit this job to the Job Tracker");
	        }
	      } 
	      catch (RemoteException e) 
	      {
	    	  System.err.println("Error occured while submitting the job");
	    	  e.printStackTrace();
	      }
	    return false;
	}

	@Override
	public void monitorandPrintJobInfo(JobConf jobConf, IRunningJobInfo job) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
	}

	/**
	 * @return the trackerRemoteRef
	 */
	public IJobTrackerServices getTrackerRemoteRef() {
		return jobTrackerServiceProvider;
	}

	/**
	 * @param trackerRemoteRef the trackerRemoteRef to set
	 */
	public void setTrackerRemoteRef(JobTrackerServiceProvider trackerRemoteRef) {
		this.jobTrackerServiceProvider = trackerRemoteRef;
	}
	
	//Acts as acknowledgment from Job-tracker that it has indeed got a request to submit a JOB
	//And it can process it. 
	private int requestJobIDfromJobTracker()
	{
		try
		{
			return this.jobTrackerServiceProvider.requestJobID();
		}
		catch(RemoteException e)
		{
			System.err.println("Jobtracker refused to take request or unable to contact JobTracker");
			return -9;
		}
	}
	
	private boolean IsJobConfValid(JobConf jobConf)
	{
		if (jobConf.getInputPath() == null)
			return false;

		if (jobConf.getOutputPath() == null)
			return false;

		if (jobConf.getJarFilePath() == null)
			return false;

		if (jobConf.getMapperClassName() == null)
			return false;

		if (jobConf.getReducerClassName() == null)
			return false;

		if (jobConf.getPartitionerClassName() == null)
			return false;

		if (jobConf.getInputFormatClassName() == null)
			return false;

		if (jobConf.getOutputFormatClassName() == null)
			return false;

		if (jobConf.getReducerNum() == 0)
			return false;

		return true;
			
	}
	
}
