/**
 * 
 */
package abhi.mapreduce;

import java.util.Comparator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;



/**
 * @author abhisheksharma
 *
 */
public class Test {
	
	
	private ConcurrentHashMap<TestClass, PriorityQueue<TestClass>> tQ;

	
	public Test()
	
	{			
		tQ = new ConcurrentHashMap<TestClass, PriorityQueue<TestClass>>();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*String[] args1 = new String[] {Test1.class.getName(), "Abhishek", "Sharma"};
		try {
			JVMUtility.startProcessinJVM(args1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		Test t = new Test();
		
		TestClass t1 = new TestClass(5);
		TestClass t2 = new TestClass(6);
		
		PriorityQueue<TestClass> instance1 = new PriorityQueue<TestClass>(10, new Comparator<TestClass>(){
            
			@Override
            public int compare(TestClass t1, TestClass t2) {
              return t1.getTest() - t2.getTest();
            }
		});
		
		instance1.add(t1);
		
		t.tQ.put(t1, instance1);
		
		PriorityQueue<TestClass> instance2 = new PriorityQueue<TestClass>(10, new Comparator<TestClass>(){
            
			@Override
            public int compare(TestClass t1, TestClass t2) {
              return t1.getTest() - t2.getTest();
            }
		});
		
		instance2.add(t1);
		
		t.tQ.put(t1, instance2);
		
		
		t1.setupTest(100);
		
		
		for(Entry<TestClass, PriorityQueue<TestClass>> entry : t.tQ.entrySet())
		{
			TestClass te = entry.getKey();
			int y = te.getTest();
			int x1 =0;
		}
		 @SuppressWarnings("static-access")
		int x = t.tQ.get("A").peek().getTest();
		int y = t.tQ.get("B").peek().getTest();
		
		int z = 0;
		
	}

}
