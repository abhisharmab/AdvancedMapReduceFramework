/**
 * 
 */
package abhi.mapreduce;

import java.util.Map;

/**
 * @author abhisheksharma
 * 
 * This interface defines a common contract to define a scheduling strategy in this Map Reduce framework
 *
 */
public interface IDefineSchedulingStrategy {

	public Map<Integer, String> makeStrategy();
}
