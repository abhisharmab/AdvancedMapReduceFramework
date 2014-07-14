/**
 * 
 */
package abhi.adfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;



/**
 * @author Douglas Rew
 *
 */
public class NameNodeSlaveMangerRequestHandler  implements Runnable {

	private NameNodeSlaveManager manger = null;
	private Socket requestSocket = null;
	
	public NameNodeSlaveMangerRequestHandler (Socket requestSocket, NameNodeSlaveManager manger)
	{
		this.requestSocket = requestSocket;
		this.manger = manger;
	}
	
	@Override
	public void run() {
		if(requestSocket.isConnected()){
			
			ObjectInputStream sockIn;
				try {
					sockIn = new ObjectInputStream(requestSocket.getInputStream());
			
				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace(); 
				}
				
		}
		
		

				
				
	
	}

}