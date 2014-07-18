/**
 * 
 */
package abhi.mapreduce;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author abhisheksharma
 *
 * Wrapper for 
 */
public class MapperPriorityQueue extends PriorityQueue<TaskTrackerInfo> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public PriorityQueue<TaskTrackerInfo> mapperTaskPriorityQueue;
	
	public MapperPriorityQueue(int initialCount)
	{
		mapperTaskPriorityQueue = new PriorityQueue<TaskTrackerInfo>(initialCount, new Comparator<TaskTrackerInfo>(){
            
			@Override
            public int compare(TaskTrackerInfo t1, TaskTrackerInfo t2) {
              if(t1.getNumOfMaps() > t2.getNumOfMaps())
              {
            	  return 1;
              }
			return 0;
            }
		});
	}
}
