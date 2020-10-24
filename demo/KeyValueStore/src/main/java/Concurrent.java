import java.io.*;
import java.net.Socket;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;


public class Concurrent extends  Thread
{
    Socket server_socket = null;
    PrintStream server_output = null;
    String data_block = "";
    String function;
    static Map<String, KeyValuePair> map= new ConcurrentHashMap<>();

//    static File newFile = new File("intermediate_data.csv");
//    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(newFile.getAbsoluteFile(), false));

    public Concurrent(Socket socket, String function) throws IOException
    {
        server_socket = socket;
        this.function = function;
//        this.map = map;
    }

    public void run() {
        Scanner server_input = null;
        try {
            server_input = new Scanner(server_socket.getInputStream());

            server_output = new PrintStream(server_socket.getOutputStream());

//            String[] tokens = null;
            String[] lines = null;
            String next_Line = "";

            while (server_input.hasNextLine()) {
                next_Line = server_input.nextLine();
                System.out.println("in concurrent " + next_Line);

                if (next_Line.contains("Writing data into map"))
                    writeDataIntoMap(next_Line);
//                else if(next_Line.equals("Clear kv-store"))
//                    clearMap(map);


//                if (next_Line.equalsIgnoreCase("NO"))
//                    break;
//                if (next_Line.equalsIgnoreCase("YES")) {
//                    next_Line = "";
//                    continue;
//                }


//                if (next_Line.equalsIgnoreCase("NO")) {
//                    map.forEach((key, value) -> {
//                        try {
//                            bufferedWriter.write(key + "," + value.getFlag() + "," + value.getExpdate() + "," + value.getLength() + "," + value.getValue() + "\n");
//                        } catch (IOException e) {
//                            System.out.println("CANNOT WRITE TO THE FILE");
//                        }
//                    });
//                    server_socket.close();
//                }

//                if (tokens[0].equalsIgnoreCase("set")) {
//                    String key = tokens[1];
//                    data_block= tokens[2];
//                    while (server_input.hasNextLine()) {
//                        data_block = server_input.nextLine();
//                        break;
//                    }
//                }

//                for(String line: lines)
//                {
//                    String[] tokens = line.split(" ");
//                    if (tokens[0].equalsIgnoreCase("set"))
//                    {
//                        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
//
////                        if (Integer.parseInt(tokens[4]) >= data_block.getBytes().length)
////                        {
//                            KeyValuePair new_pair = new KeyValuePair();
//                            new_pair.setKey(tokens[1]);
////                            new_pair.setFlag(tokens[2]);
////                            new_pair.setExpdate((tokens[3]));
////                            new_pair.setLength(Integer.parseInt(tokens[4]));
//                            String data = map.get(tokens[1]) == null ? "" : map.get(tokens[1]).getValue();
//                            new_pair.setValue(data+",1");
//                            map.put(tokens[1], new_pair);
////                            server_output.print("STORED \r\n");
//                            System.out.println("** set data "+map.get(tokens[1]).getKey()+" "+map.get(tokens[1]).getValue());
////                        }
////                        else {
////                            server_output.print("NOT STORED \r\n");
////                        }
//
////                        data_block = "";
////                        server_input = new Scanner(server_socket.getInputStream());
//                    }
//                    else if (tokens[0].equalsIgnoreCase("get")) {
//                        if (map.containsKey(tokens[1])) {
//                            server_output.print("VALUE " + tokens[1] + " " + map.get(tokens[1]).getFlag() + " " + map.get(tokens[1]).getExpdate() + " " +
//                                    map.get(tokens[1]).getLength() + "\r\n");
//                            server_output.println(map.get(tokens[1]).getValue());
//                            server_output.println("END \r\n");
//                            continue;
//
//                        } else {
//                            server_output.println("NO VALID KEY FOUND");
//                            continue;
//                        }
//                    }
            }
            System.out.println("here after sending in concurrent");
//                break;






            //bufferedWriter.close();
            server_socket.close();
        }
        catch (IOException e) {
                e.printStackTrace();
        }
    }

    public void writeDataIntoMap(String data)
    {
        String[] lines = data.split("##");
        for (String line : lines) {
            String[] tokens = line.split(" ");
            if (function.equals("WORD_COUNT") && tokens[0].equalsIgnoreCase("set")) {
                DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");

//                        if (Integer.parseInt(tokens[4]) >= data_block.getBytes().length)
//                        {
                KeyValuePair new_pair = new KeyValuePair();
                new_pair.setKey(tokens[1]);
//                            new_pair.setFlag(tokens[2]);
//                            new_pair.setExpdate((tokens[3]));
//                            new_pair.setLength(Integer.parseInt(tokens[4]));
                String temp = map.get(tokens[1]) == null ? "" : map.get(tokens[1]).getValue();
                new_pair.setValue(temp + ",1");
                map.put(tokens[1], new_pair);
//                            server_output.print("STORED \r\n");
                System.out.println("** set data " + map.get(tokens[1]).getKey() + " " + map.get(tokens[1]).getValue());
//                        }
//                        else {
//                            server_output.print("NOT STORED \r\n");
//                        }

//                        data_block = "";
//                        server_input = new Scanner(server_socket.getInputStream());
            }
            else if (function.equals("INVERTED_INDEX") && tokens[0].equalsIgnoreCase("set"))
            {
                String key = tokens[1]+" "+tokens[2];
                String val = tokens[3];
//                System.out.println("3333:"+val);
                String temp = map.get(key) == null ? "" : map.get(key).getValue();
                KeyValuePair new_pair = new KeyValuePair();
                new_pair.setKey(key);
                new_pair.setValue(temp+",1");
                map.put(key, new_pair);
                System.out.println("** set data --- key " + map.get(key).getKey() + " value " + map.get(key).getValue());
//                System.out.println(tokens[0]+" "+new_map.get(tokens[0]));
            }
        }

        if(function.equalsIgnoreCase("INVERTED_INDEX")) {

            Map<String, KeyValuePair> new_map = new ConcurrentHashMap<>(map);
            clearMap(map);

            for (String key : new_map.keySet()) {
                String[] tokens = key.split(" ");
                if (tokens.length >= 2) {
                    String k = tokens[0];
                    String value = tokens[1] + " " + new_map.get(key).getValue();

                    String temp = map.get(k) == null ? "" : map.get(k).getValue();
                    KeyValuePair keyValuePair = new KeyValuePair();
                    keyValuePair.setKey(k);
                    keyValuePair.setValue(temp + " " + value);

                    map.put(k, keyValuePair);

                    System.out.println("%%%%% key " + map.get(k).getKey() + " value " + map.get(k).getValue());
                }
            }
//            map = new_map;
        }

//        for(String key : map.keySet())
//        {
//            System.out.println("*********  key "+key+" value "+map.get(key).getValue());
//        }

        StringBuilder buffer = new StringBuilder();
        buffer.append("Map send to combiner@");
        for (String key : map.keySet()) {
            if(function.equalsIgnoreCase("WORD_COUNT"))
                buffer.append(key + " " + map.get(key).getValue() + "@");
            else
                buffer.append(key + "&" + map.get(key).getValue() + "@");
        }
        server_output.println(buffer.toString());
        System.out.println(buffer.toString());
    }

    public void clearMap(Map<String, KeyValuePair> map)
    {
        map.clear();
    }
}
