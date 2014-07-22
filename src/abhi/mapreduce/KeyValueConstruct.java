/**
 * 
 */
package abhi.mapreduce;


/**
 * @author abhisheksharma
 * 
 * The guys present the KeyValue Constructor that the Map Reduce Framework operates on. 
 * 
 * We call this class and pass the parameters to it so that we could forma  Key VAlye Construct.
 *
 */
public class KeyValueConstruct implements Comparable<KeyValueConstruct>{
	  public String key;
	  public String value;
	  public KeyValueConstruct(String key, String value){
	    this.key = key;
	    this.value = value;
	  }
	  
	  @Override
	  public int compareTo(KeyValueConstruct o) {
	    return this.key.compareTo(o.key);
	  }
	}
