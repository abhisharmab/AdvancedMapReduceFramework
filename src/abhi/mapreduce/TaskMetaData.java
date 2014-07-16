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
 * This class represents the TASK META DATA stored in the JobTracker. 
 * 
 * This meta-data encapsulates all the necessary informaion about a Task that is running on a particular node.
 *
 */
public class TaskMetaData implements Serializable{
	
	 /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// the Id of the job to which this task belongs to
	  private int jobID;

	  //The Task ID allocated by the JobTracker
	  private int taskID;
	  
	  //Type of the Task
	  private TaskType taskType;

	  //The maximum times we might try a task
	  public final static int MAXIMUM_TRIES = 1;
	  
	  //the progress and status of one task
	  private TaskProgress taskProgress;
	  
	  // the number of tries to execute this task
	  private int currentAttempts;  

	  public TaskMetaData(int jobID, int taskID, TaskType type, TaskProgress taskProgress) {
	    this.jobID = jobID;
	    this.taskID = taskID;
	    this.taskType = type;
	    this.taskProgress = taskProgress;
	    this.currentAttempts = 1;
	  }

	  public TaskMetaData(int jobID,int taskID, TaskType type) {
	    this.jobID = jobID;
	    this.taskID = taskID;
	    this.taskType = type;
	    this.currentAttempts = 1;
	  }

	  public int getJobID() {
	    return this.jobID;
	  }
	  
	  public int getTaskID() {
	    return this.taskID;
	  }

	  public TaskProgress getTaskProgress() {
	    return taskProgress;
	  }

	  public void setTaskProgress(TaskProgress taskProgress) {
	    this.taskProgress = taskProgress;
	  }

	  public void setTaskID(int taskID) {
	    this.taskID = taskID;
	  }

	  public boolean isMapperTask() {
	    return this.taskType == TaskType.MAPPER;
	  }

	  public boolean isReducerTask() {
	    return this.taskType == TaskType.REDUCER;
	  }
	  
	  public boolean isTaskDone() {
	    return this.getTaskProgress().getStatus() == TaskStatus.SUCCEEDED;
	  }
	  
	  public void increaseAttempts() {
	    this.currentAttempts ++;
	  }
	  
	  public int getAttempts() {
	    return this.currentAttempts;
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
