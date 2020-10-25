package MapReduce.Mapper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class Mapper
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
    static private String master_ip_address;
    static private int mapper;
    static private int nomapper;
    static int kv_port;

   public static void main(String args[]) throws UnknownHostException, IOException, InterruptedException {
   
	   
	   Mapper mapp=new Mapper();
	   mapp.readMapperConfig();
            mapperSocket = new Socket(master_ip_address, mapper);
            mapper_input = new Scanner(mapperSocket.getInputStream());
            mapper_output = new PrintStream(mapperSocket.getOutputStream());
           logger.info("Mapper started at port "+mapper);

            String data1="";
            
            //Accept the dataset from the master
            while(mapper_input.hasNextLine()){
                data1=mapper_input.nextLine();
                break;
            }
            data=data1;

            //Store the received data into a file
            storeKeyValue();
            mapperSocket.close();
        }
        


    //Split each line into words and write in into a file
    public static void storeKeyValue() throws IOException, InterruptedException
    {
        String[] lines = data.split(":::");

        
        String nomappers;
		File files=new File("intermediate_data.csv");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(files.getCanonicalFile(), true));

        
        	StringBuilder sb=new StringBuilder();
            for (String line : lines) {
                if(line.length() > 0 ) {
                    String[] values = line.split("###");
                    if (values.length >= 2) {
                        String fileName = values[1];
                        String[] tokens = values[0].replaceAll("[^a-zA-Z]", " ").split(" ");
                        for (int i = 0; i < tokens.length; i++) {
                            if (tokens[i].matches("[a-zA-Z]+")) {
//                                
                                if (function.equals("INVERTED_INDEX"))
                                  
                                bufferedWriter.write((tokens[i] + "," + fileName + "1" + "\n"));
                                else {
                                   
                                	
                                bufferedWriter.write((tokens[i] + "," + "1" + "\n"));
                               sb.append((tokens[i] + "," + "1" + "\n"));
                               
                                }
                            }
                        }
                    }
                }
            }
            

            mapper_output.println(sb.toString());
            mapper_output.println("Mapper exited from port "+mapper);
            System.out.println("Mapper exited from port "+mapper);
        }
    
    
    public void readMapperConfig() throws IOException
    {
       
        Properties properties = new Properties();
        BufferedReader fs= new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/mapperconfig.properties")));
        properties.load(fs);

        mapper=Integer.parseInt(properties.getProperty("MapperPort"));

        master_ip_address = properties.getProperty("MASTER_IP_ADDRESS");
        kv_port = Integer.parseInt(properties.getProperty("KV_PORT"));
        function=properties.getProperty("FUNCTION");
        fs.close();
    }

    
    
    
    }




