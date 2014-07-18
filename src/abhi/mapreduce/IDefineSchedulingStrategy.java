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

	public void makeStrategy();
}
