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
	  public final static int MAXIMUM_TRIES = 2;
	  
	  //the progress and status of one task
	  private TaskProgress taskProgress;
	  
	  // the number of tries to execute this task
	  private int currentAttempts;  

	  //Input Path - Required both for Reducers and Mappers 
	  private String inputPath; 
	  
	  //Output Path - Required both for Reducers and Mappers 
	  private String outputPath;
	  
	  private String inputFormat; 
	  
	  private String outputFormat; 
	  
	  private String partitioner;
	  
	  private String mapper; 
	  
	  private String reducer; 
	  
	  private int reducerNum;
	  
	  private int paritionNumber;
	  
	  public TaskMetaData(int jobID, int taskID, TaskType type, TaskProgress taskProgress,
			  String inputPath,String outputPath, String inputFormat, String outputFormat,
			  String mapper, String reducer, String partitioner,  int reducerNum, int paritionNumber) {
	    this.jobID = jobID;
	    this.taskID = taskID;
	    this.taskType = type;
	    this.taskProgress = taskProgress;
	    this.currentAttempts = 1;
	    this.inputPath = inputPath; 
	    this.outputPath = outputPath; 
	    this.outputFormat = outputFormat; 
	    this.inputFormat = inputFormat; 
	    this.mapper = mapper; 
	    this.reducer = reducer; 
	    this.partitioner = partitioner;
	    this.reducerNum = reducerNum;
	    this.setParitionNumber(paritionNumber);
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
	    return this.getTaskProgress().getStatus().equals(TaskStatus.SUCCEEDED);
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

	/**
	 * @return the inputPath
	 */
	public String getInputPath() {
		return inputPath;
	}

	/**
	 * @param inputPath the inputPath to set
	 */
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	/**
	 * @return the outputPath
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * @param outputPath the outputPath to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * @return the outputFormat
	 */
	public String getOutputFormat() {
		return outputFormat;
	}

	/**
	 * @param outputFormat the outputFormat to set
	 */
	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	/**
	 * @return the partitioner
	 */
	public String getPartitioner() {
		return partitioner;
	}

	/**
	 * @param partitioner the partitioner to set
	 */
	public void setPartitioner(String partitioner) {
		this.partitioner = partitioner;
	}

	/**
	 * @return the inputFormat
	 */
	public String getInputFormat() {
		return inputFormat;
	}

	/**
	 * @param inputFormat the inputFormat to set
	 */
	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	/**
	 * @return the mapper
	 */
	public String getMapper() {
		return mapper;
	}

	/**
	 * @param mapper the mapper to set
	 */
	public void setMapper(String mapper) {
		this.mapper = mapper;
	}

	/**
	 * @return the reducer
	 */
	public String getReducer() {
		return reducer;
	}

	/**
	 * @param reducer the reducer to set
	 */
	public void setReducer(String reducer) {
		this.reducer = reducer;
	}

	/**
	 * @return the reducerNum
	 */
	public int getReducerNum() {
		return reducerNum;
	}

	/**
	 * @param reducerNum the reducerNum to set
	 */
	public void setReducerNum(int reducerNum) {
		this.reducerNum = reducerNum;
	}

	/**
	 * @return the paritionNumber
	 */
	public int getParitionNumber() {
		return paritionNumber;
	}

	/**
	 * @param paritionNumber the paritionNumber to set
	 */
	public void setParitionNumber(int paritionNumber) {
		this.paritionNumber = paritionNumber;
	}
	}
