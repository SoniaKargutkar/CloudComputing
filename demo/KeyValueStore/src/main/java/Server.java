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
        System.out.println("server cons before "+port);
        this.port = port;
        System.out.println("server cons before "+this.port);
        this.function = function;
    }

    static BufferedReader br;
    static Map<String, KeyValuePair> map = new ConcurrentHashMap<>();

    public void run() {
//        File newFile = new File("intermediate_data.csv");
//        System.out.println("kv port server 11 "+this.port);
//        try {
//            br = new BufferedReader(new FileReader(newFile.getAbsoluteFile()));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }

        ServerSocket server = null;
        try {
            server = new ServerSocket(this.port);
            System.out.println("kv port server "+this.port);
//            loadIntoMap();

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


//    public static void loadIntoMap() throws IOException {
//        String line="";
//
//        while((line=br.readLine())!=null){
//            String tokens[] = line.split(",");
//            if (tokens[0]!=null && tokens[1]!=null)
//            {
//                KeyValuePair new_pair = new KeyValuePair();
//                new_pair.setKey(tokens[0]);
////                new_pair.setFlag(tokens[1]);
////                new_pair.setExpdate((tokens[2]));
////                new_pair.setLength(Integer.parseInt(tokens[3]));
//                new_pair.setValue(tokens[2]);
//                map.put(tokens[0], new_pair);
//                System.out.println("** load "+tokens[0]+" "+tokens[2]);
//            }
//        }
//    }
}



