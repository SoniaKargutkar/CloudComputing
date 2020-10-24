import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Server extends Thread
{
    int port;
    String function = "";

    public Server(int port, String function)
    {
        this.port = port;
        this.function = function;
    }

    static BufferedReader br;
    static Map<String, KeyValuePair> map = new ConcurrentHashMap<>();

    public void run() {

        ServerSocket server = null;
        try {
            server = new ServerSocket(this.port);

            while (true)
            {
                Socket serverSocket = server.accept();
                Concurrent concurrent = new Concurrent(serverSocket, function);
                concurrent.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}



