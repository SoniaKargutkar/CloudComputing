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

    public Concurrent(Socket socket, String function) throws IOException
    {
        server_socket = socket;
        this.function = function;
    }

    public void run() {
        Scanner server_input = null;
        try {
            server_input = new Scanner(server_socket.getInputStream());

            server_output = new PrintStream(server_socket.getOutputStream());

            String[] lines = null;
            String next_Line = "";

            while (server_input.hasNextLine()) {
                next_Line = server_input.nextLine();

                if (next_Line.contains("Writing data into map"))
                    writeDataIntoMap(next_Line);
            }

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


                KeyValuePair new_pair = new KeyValuePair();
                new_pair.setKey(tokens[1]);
                String temp = map.get(tokens[1]) == null ? "" : map.get(tokens[1]).getValue();
                new_pair.setValue(temp + ",1");
                map.put(tokens[1], new_pair);
            }
            else if (function.equals("INVERTED_INDEX") && tokens[0].equalsIgnoreCase("set"))
            {
                String key = tokens[1]+" "+tokens[2];
                String val = tokens[3];
                String temp = map.get(key) == null ? "" : map.get(key).getValue();
                KeyValuePair new_pair = new KeyValuePair();
                new_pair.setKey(key);
                new_pair.setValue(temp+",1");
                map.put(key, new_pair);
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
                }
            }
        }

        StringBuilder buffer = new StringBuilder();
        buffer.append("Map send to combiner@");
        for (String key : map.keySet()) {
            if(function.equalsIgnoreCase("WORD_COUNT"))
                buffer.append(key + " " + map.get(key).getValue() + "@");
            else
                buffer.append(key + "&" + map.get(key).getValue() + "@");
        }
        server_output.println(buffer.toString());
    }

    public void clearMap(Map<String, KeyValuePair> map)
    {
        map.clear();
    }
}
