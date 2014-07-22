/**
 * 
 */
package abhi.mapreduce;

import java.rmi.Remote;
import java.util.Map;

/**
 * @author abhisheksharma
 * 
 * This interface defines a common contract to define a scheduling strategy in this Map Reduce framework
 *
 */
public interface IDefineSchedulingStrategy extends Remote {

	// The Strategy Making guy needs to implement this to Make the Task Scheduling Strategy
	public void makeStrategy();
}
