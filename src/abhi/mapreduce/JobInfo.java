/**
 * 
 */
package abhi.mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import abhi.mapreduce.SystemConstants.JobStatus;

/**
 * @author abhisheksharma
 *
 /*
 * The is sort-of the lightweight Meta-Data about the Job. 
 * 
 * This information will be maintained at the JobSubmitter Level in-order to track the progress of each job
 * Also an object of JobInfo would be sent back to the client for information about the Job Progress.
 * 
 * Since this object has to be passed over the network we want to make it as light-weight as possible.
 * Although this will sort of encapsulate the logical organization of the Job it doesn't need to have 
 * all the heavy objects in it.
 *
 */
public class JobInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int jobID;
	
	private String jobName;
	
	private JobStatus jobStatus;
	
	public ConcurrentHashMap<Integer,TaskProgress> progressofallTasks;
	
	private JobConf jobConf;
	
	public JobInfo(JobConf jobConf)
	{
		this.jobConf = jobConf;
		this.setProgressofallTasks(new ConcurrentHashMap<Integer, TaskProgress>());
	}
	
	/**
	 * @return the jobID
	 */
	public int getJobID() {
		return jobID;
	}

	/**
	 * @param jobID the jobID to set
	 */
	public void setJobID(int jobID) {
		this.jobID = jobID;
	}

	/**
	 * @return the jobStatus
	 */
	public JobStatus getJobStatus() {
		return jobStatus;
	}

	/**
	 * @param jobStatus the jobStatus to set
	 */
	public void setJobStatus(JobStatus jobStatus) {
		this.jobStatus = jobStatus;
	}

	/**
	 * @return the jobName
	 */
	public String getJobName() {
		return jobName;
	}

	/**
	 * @param jobName the jobName to set
	 */
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}


	/**
	 * @return the jobConf
	 */
	public JobConf getJobConf() {
		return jobConf;
	}

	/**
	 * @param jobConf the jobConf to set
	 */
	public void setJobConf(JobConf jobConf) {
		this.jobConf = jobConf;
	}

	/**
	 * @return the progressofallTasks
	 */
	public ConcurrentHashMap<Integer,TaskProgress> getProgressofallTasks() {
		return progressofallTasks;
	}

	/**
	 * @param progressofallTasks the progressofallTasks to set
	 */
	public void setProgressofallTasks(ConcurrentHashMap<Integer,TaskProgress> progressofallTasks) {
		this.progressofallTasks = progressofallTasks;
	}
	
	public void updateTaskProgress(TaskProgress taskProgress)
	{
		int taskID = taskProgress.getTaskID(); 
		
		//Remove the old progress
		if(this.progressofallTasks.containsKey(taskID))
			this.progressofallTasks.remove(taskID);
		
		//Add the new progress
		this.progressofallTasks.put(taskID, taskProgress);
		
	}

	public boolean isJobDone()
	{
		List<TaskProgress> list = Collections.list(this.progressofallTasks.elements());
		
		for(TaskProgress t : list)
		{
			if(t.getStatus() == SystemConstants.TaskStatus.SUCCEEDED)
				continue; 
			else
				return false;
		}
		return true;
	}
}
