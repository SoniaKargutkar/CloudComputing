import java.io.IOException;
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
    Listener(ServerSocket serverSocket, List<String> messages, String dataset)
    {
        this.serverSocket = serverSocket;
        this.messages = messages;
        this.dataset=dataset;
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

            //Accept the data from Reducer and Mapper and notify the Master
            while(true)
            {
            	client_output.println(dataset);
                while(client_input.hasNextLine())
                {
                    String message = client_input.nextLine();
                    messages.add(message);
                }
                socket.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }
}
