package MapReduce.Reducer;

import java.io.*;
import java.net.Socket;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class Reducer  {

    static String data;
    static Socket reducerSocket;
    static int port;
    static Scanner reducer_input;
    static PrintStream reducer_output;
    static String function;
    static int noreducers;
    int[] reducers;
    static String master_ip_address;
    int no_reducers;
    static FileHandler fh;
    private final static Logger logger = Logger.getLogger(Reducer.class.getName());

    public static void main(String args[]) throws IOException {
    	
    	File file = new File("Logger_file");
    	if(!file.exists())
    		file.createNewFile();
    	
    	
       // File Handler and formatting for logger
        fh = new FileHandler("LogFile.log", true);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);

            reducerSocket = new Socket(master_ip_address, port);
            reducer_input = new Scanner(reducerSocket.getInputStream());
            reducer_output = new PrintStream(reducerSocket.getOutputStream());
            logger.info("Reducer started at port " + port);

            String data1 = "";

            //Accept the input data from the combiner
            while (reducer_input.hasNextLine()) {
                data1 = reducer_input.nextLine();
                break;
            }
           data = data1;

         //Add same keys to map and increment the count
            if (function.equals("WORD_COUNT")) {
                String[] kvpair = data.split("@");
                for (int i = 0; i < kvpair.length; i++) {
                    if (!kvpair[i].equalsIgnoreCase("null")) {
                        String[] tokens = kvpair[i].split(" ");
                        String[] values = tokens[1].split(",");
                        int total = 0;
                        for (String v : values) {
                            if (v.trim().equals("1"))
                                total++;
                        }
                        File files=new File("final_data"+noreducers+".csv");
                        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(files.getCanonicalFile(), true));

                            try {
                                bufferedWriter.write((tokens[0] + "," + total + "\n"));
                            } catch (IOException e) {
                                logger.severe("CANNOT WRITE TO THE FILE");
                            }
                        bufferedWriter.close();
                    }
                }} else{
                    String[] kvpair = data.split("##");
                    for (int i = 0; i < kvpair.length; i++) {
                        String[] tokens = kvpair[i].trim().split("& ");
                        if (tokens.length >= 2) {
                            String key = tokens[0];
                            String[] values = tokens[1].split(" ");

                            StringBuilder sb = new StringBuilder();
                            sb.append(key + ",");
                            for (int j = 0; j < values.length - 1; j += 2) {
                                String file1 = values[j];
                                String[] data = values[j + 1].split(",");
                                int count = 0;
                                for (String v : data) {
                                    if (v.trim().equals("1"))
                                        count++;
                                }
                                sb.append(file1 + ",");
                                sb.append(count + " ");
                                
                            }


                            File files=new File("final_data"+noreducers+".csv");
                            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(files.getCanonicalFile(), true));

                                try {
                                                                    
                                    bufferedWriter.write((sb.toString() + "\n"));
                                } catch (IOException e) {
                                    System.out.println("CANNOT WRITE TO THE FILE");
                                }
                            bufferedWriter.close();
                        }

                    }
                }
                
            logger.info("Reducer exited from port " + port);
            reducer_output.println("Reducer exited from port " + port);
       
    }
       
    
  //Read reducer configuration file
    public void readReducerConfig() throws IOException
    {
        reducers = new int[no_reducers+1];
        Properties properties = new Properties();
        BufferedReader fs= new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/reducer_config.properties")));
        properties.load(fs);

        for(int i=1; i<=no_reducers; i++)
        {
            reducers[i] = Integer.parseInt(properties.getProperty("REDUCER_"+i+"_PORT"));
        }
        master_ip_address = properties.getProperty("REDUCER_IP_ADDRESS");

        fs.close();
    }

}







