import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Mapper extends Thread
{
    static Map<String, Integer> map;
    static String data = "";
    static int port;
    static Socket socket;
    static Scanner mapper_input;
    static PrintStream mapper_output;
    static Logger logger;
    static Socket mapperSocket = null;
    static String function;


    public Mapper( int port, Logger logger, String function)
    {
        this.port = port;
        this.logger = logger;
       // this.kv_port=kv_port;
        this.function = function;
    }

    @Override
    public void run()
    {
        try {
            mapperSocket = new Socket("127.0.0.1", port);
            mapper_input = new Scanner(mapperSocket.getInputStream());
            mapper_output = new PrintStream(mapperSocket.getOutputStream());
            logger.info("Mapper started at port "+port);

            String data1="";

            //Accept the dataset from the master
            while(mapper_input.hasNextLine()){
                data1=mapper_input.nextLine();
                break;
            }
            this.data=data1;

            map = new ConcurrentHashMap();

            //Store the received data into a file
            storeKeyValue();
            mapperSocket.close();
        }
        catch (IOException | InterruptedException e) {
            logger.severe(String.valueOf(e.getStackTrace()));
        }
    }

    //Split each line into words and write in into a file
    public static void storeKeyValue() throws IOException, InterruptedException
    {
        String[] lines = data.split(":::");

        try (RandomAccessFile reader = new RandomAccessFile(new File("intermediate_data.csv"), "rw");
             FileChannel channel = reader.getChannel();
             FileLock lock = channel.lock()){

            for (String line : lines) {
                if(line.length() > 0 ) {
                    String[] values = line.split("###");
                    if (values.length >= 2) {
                        String fileName = values[1];
                        String[] tokens = values[0].replaceAll("[^a-zA-Z]", " ").split(" ");
                        for (int i = 0; i < tokens.length; i++) {
                            if (tokens[i].matches("[a-zA-Z]+")) {
                                reader.seek(reader.length());
                                if (function.equals("INVERTED_INDEX"))
                                    reader.write((tokens[i] + "," + fileName + "1" + "\n").getBytes());
                                else
                                    reader.write((tokens[i] + "," + "1" + "\n").getBytes());
                            }
                        }
                    }
                }
            }
           // lock.release();
            logger.info("Mapper exited from port "+port);
            mapper_output.println("Mapper exited from port "+port);
        }
        catch (IOException e) {
            logger.severe(String.valueOf(e.getStackTrace()));
        }
        finally {
            Runtime.getRuntime().addShutdownHook(new Thread(){public void run(){
                try {
                    mapperSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }});
        }
    }
}
