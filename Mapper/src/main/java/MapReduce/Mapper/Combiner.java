package MapReduce.Mapper;



import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Logger;

public class Combiner
{
    int no_reducer;
    Logger logger;
    String function;
    static BufferedReader br;
    static private int kv_port;
    static Map<String, String> map = new HashMap<>();
    static Socket kv_socket;
    static Scanner kv_input;
    static PrintStream kv_output;
    String[] reduced_dataset;
    List<String> list;
    static String IP;
    
    public Combiner(String IP,int no_reducer, String function,int kv_port, List<String> list)
    {
        this.no_reducer = no_reducer;
        this.logger = logger;
        this.function = function;
        this.kv_port=kv_port;
        this.list=list;
        this.IP=IP;
    }

    
    
   
    public String[] combiner() throws IOException
    {
    	
    	
        reduced_dataset = new String[no_reducer];
         File newFile = new File("intermediate_data.csv");
         br = new BufferedReader(new FileReader(newFile.getCanonicalFile()));


        System.out.println("kv port mapper "+kv_port);
        kv_socket = new Socket(IP, kv_port);
        kv_input = new Scanner(kv_socket.getInputStream());
        kv_output = new PrintStream(kv_socket.getOutputStream());

        kv_output.println("Data send to kv store from mapper from combiner");


        String line="";
        StringBuilder sb = new StringBuilder();
        sb.append("Writing data into map##");
       
        while((line=br.readLine())!=null)
        {
            String tokens[] = line.split(",");
            if (tokens.length >=2 && tokens[0]!=null && tokens[1]!=null && function.equals("WORD_COUNT"))
            {
              
                sb.append("set "+tokens[0]+" "+tokens[1]+"##");
              
            }
            else if (tokens.length >=3 && tokens[0]!=null && tokens[1]!=null && function.equals("INVERTED_INDEX"))
            {
                
                String key = tokens[0]+" "+tokens[1];
              
                sb.append("set "+key+" "+tokens[2]+"##");
               
            }
        }
        
        Map<String, String> new_map = null;
        //Convert the key (key + filename) to key and add filename to value of hashmap
        if(function.equals("INVERTED_INDEX"))
        {
            new_map = new HashMap<>();
            for(Map.Entry<String, String> entry : map.entrySet())
            {
                String[] tokens = entry.getKey().split(" ");
                String val = tokens[1] + " "+ entry.getValue()+"&";
                new_map.put(tokens[0], new_map.getOrDefault(tokens[0], "")+val);

            }
        }

        kv_output.println(sb.toString());


        while(kv_input.hasNextLine())
        {
            String nextLine = kv_input.nextLine();
            if(nextLine.contains("Map send to combiner"))
                return splitDataForReducers(nextLine);
        }


        return  reduced_dataset;
    }
    public String[] splitDataForReducers(String data) throws IOException {

        if(function.equals("WORD_COUNT"))
        {
            String[] lines = data.split("@");

            for(String l : lines) {
                if (!(l.equalsIgnoreCase("Map send to combiner"))) {
                    String[] tokens = l.split(" ");
                    
                    String str = tokens[0] + " " + tokens[1] + "@";
                    int hash = Math.abs(tokens[0].hashCode()) % no_reducer;
                  
                    if (reduced_dataset[hash] == null)
                        reduced_dataset[hash] = str;
                    else
                        reduced_dataset[hash] += str;

                    
                }
            }
        }
        else
        {
            String[] lines = data.split("@");

            for(String l : lines) {

             
                if (!(l.equalsIgnoreCase("Map send to combiner"))) {

                    String[] tokens = l.split("& ");

                    int hash = Math.abs(tokens[0].hashCode()) % no_reducer;
                 
                    if (reduced_dataset[hash] == null)
                        reduced_dataset[hash] = l+"##";
                    else
                        reduced_dataset[hash] += l+"##";

                  
                }
            }

        }

      
        return reduced_dataset;
    }
}

