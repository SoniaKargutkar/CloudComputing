package MapReduce.Master;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;

public class Listener extends Thread
{
    ServerSocket serverSocket;
    List<String> messages;
    String dataset;
    List<String> list;
    Listener(ServerSocket serverSocket, List<String> messages, String dataset, List<String> list)
    {
    	System.out.println("In Listener");
        this.serverSocket = serverSocket;
        this.messages = messages;
        this.dataset=dataset;
        this.list = list;
    }

    public void run()
    {
        Scanner client_input = null;
        try
        {
            //Create a server socket to accept the Mapper and Reducer socket connection
            Socket socket = serverSocket.accept();
            client_input = new Scanner(socket.getInputStream());
            PrintStream client_output = new PrintStream(socket.getOutputStream());
            
            System.out.println("In listener");

            //Accept the data from Reducer and Mapper and notify the Master
            while(true)
            {
            	client_output.println(dataset);
                while(client_input.hasNextLine())
                {
                    String message = client_input.nextLine();
                    System.out.println("messages"+messages);
                    if(!message.contains("Mapper exited from port") || !message.contains("Reducer exited from port"))
                    {
                    	list.add(message);
                    }
                    else {
                    messages.add(message);
                    System.out.println("messages"+messages);
                    
                    }
                    
                }
                
//                FileInputStream file = new FileInputStream 
//                        ("intermediate_data.ser"); 
//                ObjectInputStream ois = new ObjectInputStream(file);
//                 Object test = ois.readObject();
//        ObjectInputStream in = new ObjectInputStream 
//                        (file); 
////
////        //Method for deserialization of object 
////         object = in.readObject(); 
//                
//
//                socket.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        } 

    }
    
    
    
   
        
   
}

