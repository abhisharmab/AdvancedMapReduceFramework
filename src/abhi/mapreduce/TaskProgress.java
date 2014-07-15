/**
 * 
 */
package abhi.mapreduce;

import java.io.Serializable;

import abhi.mapreduce.SystemConstants.TaskStatus;
import abhi.mapreduce.SystemConstants.TaskType;

/**
 * @author abhisheksharma
 *
 * This class represents the Progress of the each of the Tasks running on the Nodes. 
 * We represent progress in-terms of how much percentage of work is done.
 *
 */
public class TaskProgress implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	//Percentage of work done
	private float percentageCompleted;
	
	//The taskID whos progress this class represents
	private int taskID;
	
	//Whether this is a Mapper or a Reducer Task
	private TaskType taskType;
	
	//The current status of the Task
	private TaskStatus taskStatus;
	
	//Latest TimeStamp when this was updated
	private long latestUpdateTimeStamp;
	
	public TaskProgress(int taskId, TaskType type)
	{
		this.taskID = taskId;
		this.taskType = type;
		this.taskStatus = TaskStatus.INITIALIZED;
	}

	/**
	 * @return the percentageCompleted
	 */
	public float getPercentageCompleted() {
		return percentageCompleted;
	}

	/**
	 * @param percentageCompleted the percentageCompleted to set
	 */
	public void setPercentageCompleted(float percentageCompleted) {
		this.percentageCompleted = percentageCompleted;
	}

	/**
	 * @return the taskID
	 */
	public int getTaskID() {
		return taskID;
	}

	/**
	 * @param taskID the taskID to set
	 */
	public void setTaskID(int taskID) {
		this.taskID = taskID;
	}

	/**
	 * @return the status
	 */
	public TaskStatus getStatus() {
		return taskStatus;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(TaskStatus status) {
		this.taskStatus = status;
	}

	/**
	 * @return the latestUpdateTimeStamp
	 */
	public long getLatestUpdateTimeStamp() {
		return latestUpdateTimeStamp;
	}

	/**
	 * @param latestUpdateTimeStamp the latestUpdateTimeStamp to set
	 */
	public void setLatestUpdateTimeStamp(long latestUpdateTimeStamp) {
		this.latestUpdateTimeStamp = latestUpdateTimeStamp;
	}

	/**
	 * @return the taskType
	 */
	public TaskType getTaskType() {
		return taskType;
	}

	/**
	 * @param taskType the taskType to set
	 */
	public void setTaskType(TaskType taskType) {
		this.taskType = taskType;
	}

}
