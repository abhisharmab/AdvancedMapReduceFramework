/**
 * 
 */
package abhi.mapreduce;

import java.io.Serializable;

/**
 * @author abhisheksharma
 *
 * This class is map/reduce job configuration used by the application programmer to pass the job configuration.
 * 
 * JobConf is the primary interface for a user to describe a 
 * map-reduce job to the map/reduce framework for execution.
 * 
 * This information is used to pass around and describes things about the Job that needs to processed.
 *
 */
public class JobConf implements Serializable{

	private static final long serialVersionUID = 1L;

	private int reducerNum;

	  // the mapper class name
	  private String mapperClassName;

	  // the reducer class name
	  private String reducerClassName;

	  private String partitionerClassName; 
	  //User specified class on how to partition the output from the Mapper
	  //Generally a HASH function will be used by the application programmer.
	  
	  private String inputFormatClassName;
	  
	  private String outputFormatClassName;
	  
	  private int jobID;

	  private String jobName;

	  private String inputPath;

	  private String outputPath;
	  
	  // the path of the jar file which contains all the codes of a job
	  private String jarFilePath;
	  
	  private String jobRequestOriginHostName;

	  //Constructor for JobConf
	  public JobConf() {
		    this.setJobName("");
		    this.setInputPath(null);
		    this.setOutputPath(null);
		    this.setMapperClassName(null);
		    this.setReducerClassName(null);
		    this.setPartitionerClassName(null);
		    this.setPartitionerClassName(null);
		    this.setInputFormatClassName(null);
		    this.setOutputFormatClassName(null);
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
	 * @return the mapperClassName
	 */
	public String getMapperClassName() {
		return mapperClassName;
	}

	/**
	 * @param mapperClassName the mapperClassName to set
	 */
	public void setMapperClassName(String mapperClassName) {
		this.mapperClassName = mapperClassName;
	}

	/**
	 * @return the reducerClassName
	 */
	public String getReducerClassName() {
		return reducerClassName;
	}

	/**
	 * @param reducerClassName the reducerClassName to set
	 */
	public void setReducerClassName(String reducerClassName) {
		this.reducerClassName = reducerClassName;
	}

	/**
	 * @return the partitionerClassName
	 */
	public String getPartitionerClassName() {
		return partitionerClassName;
	}

	/**
	 * @param partitionerClassName the partitionerClassName to set
	 */
	public void setPartitionerClassName(String partitionerClassName) {
		this.partitionerClassName = partitionerClassName;
	}

	/**
	 * @return the inputFormatClassName
	 */
	public String getInputFormatClassName() {
		return inputFormatClassName;
	}

	/**
	 * @param inputFormatClassName the inputFormatClassName to set
	 */
	public void setInputFormatClassName(String inputFormatClassName) {
		this.inputFormatClassName = inputFormatClassName;
	}

	/**
	 * @return the outputFormatClassName
	 */
	public String getOutputFormatClassName() {
		return outputFormatClassName;
	}

	/**
	 * @param outputFormatClassName the outputFormatClassName to set
	 */
	public void setOutputFormatClassName(String outputFormatClassName) {
		this.outputFormatClassName = outputFormatClassName;
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
	 * @return the jarFilePath
	 */
	public String getJarFilePath() {
		return jarFilePath;
	}

	/**
	 * @param jarFilePath the jarFilePath to set
	 */
	public void setJarFilePath(String jarFilePath) {
		this.jarFilePath = jarFilePath;
	}


	/**
	 * @return the jobRequestOriginHostName
	 */
	public String getJobRequestOriginHostName() {
		return jobRequestOriginHostName;
	}


	/**
	 * @param jobRequestOriginHostName the jobRequestOriginHostName to set
	 */
	public void setJobRequestOriginHostName(String jobRequestOriginHostName) {
		this.jobRequestOriginHostName = jobRequestOriginHostName;
	}
	
	
}
