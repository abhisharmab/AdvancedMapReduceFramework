/**
 * 
 */
package abhi.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;

import abhi.adfs.NameNodeSlave;

/**
 * @author abhisheksharma
 *
 * This Thread is the LiveStatus MonitorThread for the Job that the User Submitted. 
 * 
 * The main function of this Thread are as follows: 
 * 	1. Print the JobStatus in RunTime 
 * 	2. Move the Final Results file to Requested Directory if Job was successfull
 */
public class LiveStatusThread implements Runnable {

	/**
	 * 
	 */

	private int jobId; 

	private IJobTrackerServices jobTrackerServiceReference;

	private String finalOutputPath; 

	private NameNodeSlave nameNodeSlaveReference;
	
	boolean notTimeToExit; 

	public LiveStatusThread(int jobId, IJobTrackerServices jtSReference, String finalOutputPath, NameNodeSlave nameNodeSlaveReference) {
		this.setJobId(jobId);
		this.setJobTrackerServiceReference(jtSReference);
		this.finalOutputPath = finalOutputPath;
		this.nameNodeSlaveReference = nameNodeSlaveReference;
		this.notTimeToExit = true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() 
	{
		try 
		{
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		JobInfo liveJobInfo;

		while(notTimeToExit)
		{
			try {
				liveJobInfo = this.jobTrackerServiceReference.getLiveStatusofJob(this.jobId);
				Collection<TaskProgress> pList = liveJobInfo.getProgressofallTasks().values();

				if(liveJobInfo.getJobStatus() == SystemConstants.JobStatus.SUCCEEDED)
				{
					notTimeToExit = false; 
					System.out.println("Job_" + liveJobInfo.getJobName() + "Successfully Completed. mapPhase = 100%, reducePhase = 100%");

					System.out.println("Moving Final Results File/s to User Requested Location.....");

					//Create the directory for the Final Output
					File dir = new File(this.finalOutputPath);

					boolean good = dir.mkdir();

					if(good)
					{
						int counter = 0;
						for(TaskProgress tprogress : pList)
						{
							counter ++;
							if(tprogress.getTaskType() == SystemConstants.TaskType.REDUCER)
							{
								//Actually create the requested directory
								//tprogress.getCreatedFileNames().get(0)
								String fileName = tprogress.getCreatedFileNames().get(0);
								String fileContent;
								
								try 
								{
									fileContent = this.nameNodeSlaveReference.retrieveFromLocalDataNode(fileName);

									File file;
									
									if(pList.size() > 1)
										file = new File(this.finalOutputPath + System.getProperty("file.separator") + "final_result");
									else
										file = new File(this.finalOutputPath + System.getProperty("file.separator") + "final_result_" + counter);

									file.createNewFile();
								
									FileWriter fw = new FileWriter(file.getAbsoluteFile());
									BufferedWriter bw = new BufferedWriter(fw);
									bw.write(fileContent);
									bw.close();
						 
								} catch (IOException e) {
									e.printStackTrace();
								}

							} 
						}
					}
					
					System.out.println("DONE. Final Results Moved to the Requested Location");
				}
				
				else if(liveJobInfo.getJobStatus() == SystemConstants.JobStatus.FAILED)
				{
					notTimeToExit = false; 
					System.out.println("Job" + liveJobInfo.getJobName() + " Failed :(. System Error. Please try again");
				}

				else
				{
					int noMappers = 0;
					float cumulativeMapPercent = 0;

					int noReducers = 0;
					float cumulativeReducePercent = 0;

					for(TaskProgress tprogress : pList)
					{
						if(tprogress.getTaskType() == SystemConstants.TaskType.MAPPER)
						{
							noMappers++;
							cumulativeMapPercent = tprogress.getPercentageCompleted();
						}
						else
						{
							noReducers++;
							cumulativeReducePercent = tprogress.getPercentageCompleted();
						}
					}

					if(noMappers == 0)
						noMappers = 1; 
					if(noReducers == 0)
						noReducers = 1;

					System.out.println("Job_" + liveJobInfo.getJobName() + ". mapPhase = " + cumulativeMapPercent/noMappers + ", reducePhase = " + cumulativeReducePercent/noReducers);
				}

				try 
				{
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			

			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
			
	}

	/**
	 * @return the jobTrackerServiceReference
	 */
	public IJobTrackerServices getJobTrackerServiceReference() {
		return jobTrackerServiceReference;
	}

	/**
	 * @param jtSReference the jobTrackerServiceReference to set
	 */
	public void setJobTrackerServiceReference(IJobTrackerServices jtSReference) {
		this.jobTrackerServiceReference = jtSReference;
	}

	/**
	 * @return the jobId
	 */
	public int getJobId() {
		return jobId;
	}

	/**
	 * @param jobId the jobId to set
	 */
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

}
