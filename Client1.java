

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client1 
{ 
	final static int port = 56192; 

	public static void main(String args[]) throws UnknownHostException, IOException 
	{ 		
		// getting server IP address 
		InetAddress ip = InetAddress.getByName("localhost"); 
		
		// establish the connection 
		Socket s = new Socket(ip, port); 
		
		// getting input and output streams 
		DataInputStream instream = new DataInputStream(s.getInputStream()); 
		DataOutputStream outstream = new DataOutputStream(s.getOutputStream()); 
		
		String[] setArray = {"set ashok 12", "set concelia 12", "set amrita 12" };
		String[] getArray = {"get ashok", "get concelia", "get amrita"};
                String[] valuesArray = {"father", "mother", "sister"};

                // Thread for reading incoming messages
		Thread thread = new Thread(new Runnable() 
		{ 
			@Override
			public void run() { 

				while(true) { 
					try { 
						// read the message sent to this client 
						String msg = instream.readUTF();
						System.out.println(msg); 
					} catch (Exception e) { 
						System.out.println("Logging out");
						break;
					} 
				} 
			} 
		}); 
                
		// Thread for sending messages 
		Thread thsend = new Thread(new Runnable() 
		{ 
			@Override
			public void run() { 
				for(int i = 0; i < valuesArray.length ; i++) { 
					
					try { 
						// write on the output stream 				
						outstream.writeUTF(setArray[i]);						
						outstream.writeUTF(valuesArray[i]);
						outstream.writeUTF(getArray[i]);
					} catch (Exception e) {  
						System.out.println("Logging out");
						break;
					} 
				} 
			} 
		}); 
		
		thsend.start(); 
		thread.start(); 

	} 
} 
