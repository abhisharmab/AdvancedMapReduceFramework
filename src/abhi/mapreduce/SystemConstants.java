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
	
	private static final ResourceBundle sysConfig = ResourceBundle.getBundle("Configuration", Locale.getDefault());
	
	//----Many of the System Configuration and System Constants are Declared here --//
	public static final String REGISTRY_HOST = "REGISTRY.HOST";
	public static final String REGISTRY_PORT = "REGISTRY.PORT";
	public static final String JOBTRACKER_SERVICE_NAME = "JOBTRACKER.SERVICE";
	public static final String FILE_PARITION_SIZE = "FILE.PARTITION.SIZE";
	public static final String REPLICATION = "REPLICATION";

	
	public static String getConfig(String configName) 
	{
		return sysConfig.getString(configName);
	}

	
}
