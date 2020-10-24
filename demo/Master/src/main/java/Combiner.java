import java.io.*;
import java.net.Socket;
import java.util.HashMap;
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

    public Combiner(int no_reducer, Logger logger, String function,int kv_port)
    {
        this.no_reducer = no_reducer;
        this.logger = logger;
        this.function = function;
        this.kv_port=kv_port;
    }

    public String[] combiner() throws IOException
    {
        reduced_dataset = new String[no_reducer];
         File newFile = new File("intermediate_data.csv");
         br = new BufferedReader(new FileReader(newFile.getCanonicalFile()));


        System.out.println("kv port mapper "+kv_port);
        kv_socket = new Socket("127.0.0.1", kv_port);
        kv_input = new Scanner(kv_socket.getInputStream());
        kv_output = new PrintStream(kv_socket.getOutputStream());

        kv_output.println("Data send to kv store from mapper from combiner");


        String line="";
        StringBuilder sb = new StringBuilder();
        sb.append("Writing data into map##");
        System.out.println("buffer size: "+newFile.length());
        while((line=br.readLine())!=null)
        {
            String tokens[] = line.split(",");
            if (tokens.length >=2 && tokens[0]!=null && tokens[1]!=null && function.equals("WORD_COUNT"))
            {
               // map.put(tokens[0], map.getOrDefault(tokens[0], "")+",1");
                sb.append("set "+tokens[0]+" "+tokens[1]+"##");
                System.out.println("*** comb set "+tokens[0]+" 1");
            }
            else if (tokens.length >=3 && tokens[0]!=null && tokens[1]!=null && function.equals("INVERTED_INDEX"))
            {
                //if the function is INVERTED_INDEX, create key as (key + filename)
                String key = tokens[0]+" "+tokens[1];
                System.out.println("buffer size in inverted index: "+newFile.length());
                sb.append("set "+key+" "+tokens[2]+"##");
                System.out.println("*** comb set "+key+" 1");
               // map.put(key, map.getOrDefault(key, "")+",1");
//                System.out.println(key+" "+map.get(key));
            }
        }

        System.out.println("buffer size after: "+br.read());
//        br.close();
        kv_output.println(sb.toString());


        while(kv_input.hasNextLine())
        {
            String nextLine = kv_input.nextLine();
            if(nextLine.contains("Map send to combiner"))
                return splitDataForReducers(nextLine);
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
//                System.out.println(tokens[0]+" "+new_map.get(tokens[0]));
            }
        }

//        String[] reduced_dataset = new String[no_reducer];
//
//        //Create a hashcode for each and assign it to the reducers
//        if(function.equals("WORD_COUNT"))
//        {
//            for(Map.Entry<String, String> entry : map.entrySet())
//            {
//                String str = entry.getKey()+" "+entry.getValue()+"@";
//                int hash = Math.abs(entry.getKey().hashCode())%no_reducer;
//                if(reduced_dataset[hash] == null)
//                    reduced_dataset[hash] = str;
//                else
//                    reduced_dataset[hash] += str;
//            }
//        }
//        else
//        {
//            for(Map.Entry<String, String> entry : new_map.entrySet())
//            {
//                String str = entry.getKey()+"#"+entry.getValue()+"@";
//                int hash = Math.abs(entry.getKey().hashCode())%no_reducer;
//                if(reduced_dataset[hash] == null)
//                    reduced_dataset[hash] = str;
//                else
//                    reduced_dataset[hash] += str;
//            }
//        }
//
////        for(String dataset : reduced_dataset)
////        {
////            System.out.println(dataset);
////        }

        return  reduced_dataset;
    }
    public String[] splitDataForReducers(String data) throws IOException {

        if(function.equals("WORD_COUNT"))
        {
            String[] lines = data.split("@");

            for(String l : lines) {
                if (!(l.equalsIgnoreCase("Map send to combiner"))) {
                    System.out.println(l);
                    String[] tokens = l.split(" ");
                    System.out.println("Tokens:"+tokens[0]+"token 2"+tokens[1]);
                    String str = tokens[0] + " " + tokens[1] + "@";
                    int hash = Math.abs(tokens[0].hashCode()) % no_reducer;
                    System.out.println("Hash Value: "+hash);
                    if (reduced_dataset[hash] == null)
                        reduced_dataset[hash] = str;
                    else
                        reduced_dataset[hash] += str;

                    System.out.println(reduced_dataset[hash]);
                }
            }
        }
        else
        {
            String[] lines = data.split("@");

            for(String l : lines) {

                System.out.println("########### lines"+l);

                if (!(l.equalsIgnoreCase("Map send to combiner"))) {
//                    System.out.println(l);
                    String[] tokens = l.split("& ");
//                    System.out.println("Tokens:"+tokens[0]+"token 2"+tokens[1]);
//                    String str = tokens[0] + "&" + tokens[1] + " " + tokens[2] + "@";
                    int hash = Math.abs(tokens[0].hashCode()) % no_reducer;
                    System.out.println("Hash Value: "+hash);
                    if (reduced_dataset[hash] == null)
                        reduced_dataset[hash] = l+"##";
                    else
                        reduced_dataset[hash] += l+"##";

                    System.out.println("Reduced dataset  "+reduced_dataset[hash]);
                }
            }




//            kv_output.println("Clear kv-store");



//            for(Map.Entry<String, String> entry : new_map.entrySet())
//            {
//                String str = entry.getKey()+"#"+entry.getValue()+"@";
//                int hash = Math.abs(entry.getKey().hashCode())%no_reducer;
//                if(reduced_dataset[hash] == null)
//                    reduced_dataset[hash] = str;
//                else
//                    reduced_dataset[hash] += str;
//            }
        }

        for(String s : reduced_dataset)
        {
            System.out.println("#####:"+s);
        }

//        br.close();
        return reduced_dataset;
    }
}
