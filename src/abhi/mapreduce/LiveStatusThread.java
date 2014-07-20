/**
 * 
 */
package abhi.mapreduce;

import java.util.Collection;
import java.util.List;

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
	
	public LiveStatusThread(int jobId, IJobTrackerServices jtSReference) {
		this.setJobId(jobId);
		this.setJobTrackerServiceReference(jtSReference);
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() 
	{
		boolean notTimeToExit = true; 
		JobInfo liveJobInfo;
		
		while(notTimeToExit)
		{
			liveJobInfo = this.jobTrackerServiceReference.getLiveStatusofJob(this.jobId);
			Collection<TaskProgress> pList = liveJobInfo.getProgressofallTasks().values();

			if(liveJobInfo.getJobStatus() == SystemConstants.JobStatus.SUCCEEDED)
			{
				notTimeToExit = false; 
				System.out.println("Job_" + liveJobInfo.getJobName() + "Successfully Completed. mapPhase = 100%, reducePhase = 100%");
				
				System.out.println("Moving Final Results File/s to User Requested Location.....");
				//Do some code to move file to that location 
				System.out.println("DONE :)");
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
				Thread.sleep(6000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
