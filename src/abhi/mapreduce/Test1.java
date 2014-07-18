/**
 * 
 */
package abhi.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

/**
 * @author abhisheksharma
 *
 */
public class Test1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String firstName = args[0];
		String lastName = args[1];
		
		PrintStream out;
		try {
			out = new PrintStream(new FileOutputStream(new File(SystemConstants.getConfig(SystemConstants.ADFS_DIRECTORY)+ "tasklog")));
			System.setOut(out);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		System.out.println(firstName + lastName);
	}

}
