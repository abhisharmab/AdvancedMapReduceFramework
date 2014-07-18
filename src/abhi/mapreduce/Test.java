/**
 * 
 */
package abhi.mapreduce;


/**
 * @author abhisheksharma
 *
 */
public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] args1 = new String[] {Test1.class.getName(), "Abhishek", "Sharma"};
		try {
			Utility.startProcessinJVM(args1, 10);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
