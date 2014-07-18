/**
 * 
 */
package abhi.mapreduce;


/**
 * @author abhisheksharma
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
