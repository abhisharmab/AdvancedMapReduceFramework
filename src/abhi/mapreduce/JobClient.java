/**
 * 
 */
package abhi.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import abhi.adfs.NameNodeMaster;
import abhi.adfs.NameNodeSlave;

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
	private NameNodeSlave nameNodeSlaveReference;

	public JobClient()
	{
		try
		{
			int registryPort = Integer.parseInt(SystemConstants.getConfig(SystemConstants.JOBTRACKER_REGISTRY_PORT));
			Registry jobTrackerRmiRegistry = LocateRegistry.getRegistry(SystemConstants.getConfig(SystemConstants.JOBTRACKER_REGISTRY_HOST),registryPort);
			this.jobTrackerServiceProvider = (IJobTrackerServices) jobTrackerRmiRegistry.lookup(SystemConstants.getConfig(SystemConstants.JOBTRACKER_SERVICE_NAME));
		}
		catch(NumberFormatException | RemoteException | NotBoundException e)
		{
			System.out.println("Error occurred in communcating with JobTracker");
			System.out.println("Ensure Jobtracker is running and check configuration");
		}

		try
		{
			//Get the NameNodeMaster
			int registryPort = Integer.parseInt(SystemConstants.getConfig(SystemConstants.NAMENODE_REGISTRY_PORT));
			Registry nameNodeRmiRegistry = LocateRegistry.getRegistry(SystemConstants.getConfig(SystemConstants.NAMENODE_REGISTRY_HOST),registryPort);
			this.nameNodeMasterReference = (NameNodeMaster) nameNodeRmiRegistry.lookup(SystemConstants.getConfig(SystemConstants.NAMENODE_SERVICE_NAME));
	        
			//Get the NameNodeSlave
			String identifer = InetAddress.getLocalHost().getHostName();
	        String slave_Name = SystemConstants.getConfig(SystemConstants.NAMENODE_SLAVE_SERVICE);
	        String lookupName = slave_Name +"_" + identifer;
        
        	this.nameNodeSlaveReference =  (NameNodeSlave) LocateRegistry.getRegistry().lookup(lookupName);		
			
		}
		catch(NumberFormatException | RemoteException | NotBoundException | UnknownHostException e)
		{
			System.out.println("Error occurred in communcating with NameNode via the Registry");
		}

	}

	public static void main(String[] args)
	{
		JobClient jobClient = new JobClient();
	}

	@Override
	public boolean submitJob(JobConf jobConf) throws FileNotFoundException, IOException {
		//1. Check if the Job Configuration is Valid
		if(jobConf == null || !IsJobConfValid(jobConf))
		{
			System.out.println("Invalid Job Configuration Submitted. Please check your Job Source Code and Config");
			return false;
		}

		boolean IsFilePartitioned = false;

		if(!nameNodeMasterReference.checkFileExistence(jobConf.getInputPath()))
		{
			//Request to Partition the File
			IsFilePartitioned = this.nameNodeSlaveReference.dump(jobConf.getInputPath());
		}
		else
		{
			IsFilePartitioned = true;
		}

		if(IsFilePartitioned)
		{
			// If the file is broken-up and ready. No-worries then. Proceed with sending command to JobTracker 
			//Piggyback on this JobId to Report Progress for the Client about the Job that he request to Run
			int uniqueJobID = requestJobIDfromJobTracker();
			if(uniqueJobID <= 0)
			{
				System.out.println("The system is not available for submitting new job.");
				return false;
			} else {
				jobConf.setJobID(uniqueJobID);
				jobConf.setJobRequestOriginHostName(InetAddress.getLocalHost().getHostName());
			}

			if(jobConf.getJobName() == null || jobConf.getJobName().length() == 0)
				jobConf.setJobName(String.valueOf(uniqueJobID));

			try {
				if (this.jobTrackerServiceProvider.submitJob(jobConf)) 	    
				{
					System.out.println("JobClient submmited Job successfully. Monitoring status...");

					this.monitorandPrintJobInfoandMoveFile(uniqueJobID,jobConf.getOutputPath());

					return true;
				}
				else
				{
					System.out.println("Failed to submit this job to the Job Tracker");
				}
			} 
			catch (RemoteException | InterruptedException e) 
			{
				System.out.println("Error occured while submitting the job");
				e.printStackTrace();
			}
			return false;
		}
		else
		{
			System.out.println("Unable to partition the file. Either the file is not present or file is corrupt");
			return false;
		}
	}

	@Override
	public void monitorandPrintJobInfoandMoveFile(int uniqueJobID, String outputPath) throws IOException,
	InterruptedException {

		Thread progressMonitorThread = new Thread(new LiveStatusThread(uniqueJobID,this.jobTrackerServiceProvider, outputPath, this.nameNodeSlaveReference));
		progressMonitorThread.start();
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

	@Override
	public boolean putFinalPayload() {
		// TODO Auto-generated method stub
		return false;
	}


}
