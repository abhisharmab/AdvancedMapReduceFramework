/**
 * 
 */
package abhi.mapreduce;

/**
 * @author abhisheksharma
 *
 */
public class TestClass {
	
	
	private static int test; 
	
	public TestClass(int test)
	{
		TestClass.test = test;		
	}

	/**
	 * @return the test
	 */
	public static int getTest() {
		return TestClass.test;
	}

	/**
	 * @param test the test to set
	 */
	public static void setTest(int test) {
		TestClass.test = test;
	}

	
	public void setupTest(int x)
	{
		TestClass.test = x;
	}
}
