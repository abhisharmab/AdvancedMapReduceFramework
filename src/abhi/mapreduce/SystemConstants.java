/**
 * 
 */
package abhi.mapreduce;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * @author abhisheksharma
 *
 *This class is for all the constants to be defined for the MapReduce framework 
 *Having all the constants in one place will make the editing and chaning of things much easier. 
 */
public class SystemConstants {
	
	public enum JobStatus 
	{
		    INITIALIZED, 
		    INPROGRESS, 
		    FAILED, 
		    SUCCEEDED
	}
	
	public enum TaskType 
	{
		    MAPPER, 
		    REDUCER
	}
	
	public enum TaskStatus
	{
	    INITIALIZED, 
	    INPROGRESS, 
	    FAILED, 
	    SUCCEEDED
	}
	
	public enum MapJobsStatus
	{	    
	    INPROGRESS, 
	    FAILED, 
	    SUCCEEDED
	}
	private static final ResourceBundle sysConfig = ResourceBundle.getBundle("Configuration", Locale.getDefault());
	
	//----Many of the System Configuration and System Constants are Declared here --//
	public static final String REGISTRY_HOST = "REGISTRY.HOST";
	public static final String REGISTRY_PORT = "REGISTRY.PORT";
	public static final String JOBTRACKER_SERVICE_NAME = "JOBTRACKER.SERVICE";
	public static final String FILE_PARITION_SIZE = "FILE.PARTITION.SIZE";
	public static final String REPLICATION = "REPLICATION";
	public static final String NAMENODE_SERVICE_NAME = "NAMENODE.SERVICE";
	public static final String ADFS_DIRECTORY="ADFS.DIRECTORY"; 
	public static final String JAR_DIRECTORY="JAR.DIRECTORY";
	
	
	public static final String NAMENODE_REGISTRY_HOST = "NAMENODE.REGISTRY.HOST";
	public static final String NAMENODE_REGISTRY_PORT = "NAMENODE.REGISTRY.PORT";
	
	
	public static final String JOBTRACKER_REGISTRY_HOST = "JOBTRACKER.REGISTRY.HOST";
	public static final String JOBTRACKER_REGISTRY_PORT = "JOBTRACKER.REGISTRY.PORT";
	
	public static final String MAX_MAPPER_SLOTS = "MAX.MAPPER.SLOTS";	
	public static final String MAX_REDUCER_SLOTS = "MAX.REDUCER.SLOTS";

    public static final String RMI_CODE_BASE="rmi.code.base";

	
	public static String getConfig(String configName) 
	{
		return sysConfig.getString(configName);
	}

	
}
