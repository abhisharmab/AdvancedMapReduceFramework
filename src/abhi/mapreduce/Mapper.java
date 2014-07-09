/**
 * 
 */
package abhi.mapreduce;

import java.io.IOException;

/**
 * @author abhisheksharma
 * 
 * This class is the abstract implementation of the Mapper Class which defines the policy of using the Map Reduce framework provided
 * When a programmer uses the this Map-Reduce Framework he will have to extend this Mapper class 
 * and implement the respective required methods 
 * 
 * Application Programmer will provide the precise implementations for these based on the KIND of Job
 */

public abstract class Mapper {

	//Called once at the beginning to setup the Map Task or perform some pre-processing as per Application Need
	protected void setup() throws IOException, InterruptedException {};
	
	//Called once at the end (Possibly for some cleanup and housekeeping work)
	protected void cleanUp() throws IOException, InterruptedException{};
	
	
	protected void map (String key, String value, OuputCollector outputCollector, Reporter report)
			throws IOException, InterruptedException
	{
		//To be over-ridden/implemented at the 
	}
}
