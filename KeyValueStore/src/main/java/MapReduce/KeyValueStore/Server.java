package MapReduce.KeyValueStore;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Server
{
    static int port;
    static String function = "";
   


    static BufferedReader br;
    static Map<String, KeyValuePair> map = new ConcurrentHashMap<>();

    public static void main(String[] args) {

    	try {
    		Server serverObj = new Server();
			serverObj.readConfigFile();
		
        ServerSocket server = null;
        
            server = new ServerSocket(port);
      


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


    
    public void readConfigFile() throws IOException
    {
        Properties properties = new Properties();
        BufferedReader fs= new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/master_config.properties")));
        properties.load(fs);

       
       
        port = Integer.parseInt(properties.getProperty("KV_PORT"));
        function = properties.getProperty("FUNCTION");
        fs.close();
    }

}




