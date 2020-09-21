package clientserver;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
//import java.util.Hashtable;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class Server {

    public static void main(String[] args) throws IOException {

        String key, value, str;
        //Creating a server socket and binding it to the specified local port number
        ServerSocket ss = new ServerSocket(56192, 0, InetAddress.getByName("localhost"));
        //Creating a socket to listen to and pass data to clients
        Socket s;
        ConcurrentHashMap<String, String> hmap = new ConcurrentHashMap<>();
        //HashMap<String, String> map = new HashMap<String,String>();
        //Hashtable to store keys and values
        //HashTable<String, String> ht = new HashTable<String,String>();
        
        //Writing data into specified text file
        FileWriter fw = new FileWriter("KeyValueStore.txt", true);

        try {
            File f = new File("KeyValueStore.txt");
            BufferedReader br = new BufferedReader(new FileReader(f));
            while ((str = br.readLine()) != null) {
                key = str.split(":", 2)[0];
                value = str.split(":", 2)[1];
                System.out.println("key" + key + "\n" + "value" + value); //check
                hmap.put(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Infinite loop for server to listen for clients 
        while (true) {
            try {
                System.out.println("Listening for clients on port 56192");
                s = ss.accept();
                System.out.println("Client request : " + s);

                DataInputStream instream = new DataInputStream(s.getInputStream());
                DataOutputStream outstream = new DataOutputStream(s.getOutputStream());

                System.out.println("Creating a new handler for this client " + s);

                clientserver.ClientHandler newClient = new clientserver.ClientHandler(s, hmap, fw, instream, outstream);

                //Starting a new thread for handling client
                Thread t = new Thread(newClient);
                t.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class ClientHandler implements Runnable {

    String key, value, clientdata;
    //Regex patterns for SET and GET commands from clients
    String setPattern = "^set [\\w]+ [\\d ]+$";
    String getPattern = "^get [\\w]+$";

    ConcurrentHashMap<String, String> hmap;
    FileWriter fw;
    DataInputStream instream;
    DataOutputStream outstream;
    Socket s;
    Scanner scn = new Scanner(System.in);

    public ClientHandler(Socket s, ConcurrentHashMap<String, String> hmap, FileWriter fw, DataInputStream instream, DataOutputStream outstream) {

        this.s = s;
        this.hmap = hmap;
        this.fw = fw;
        this.instream = instream;
        this.outstream = outstream;

    }

    @Override
    public void run() {
        while (true) {
            try {
                clientdata = instream.readUTF();
                //SET

                if (Pattern.compile(setPattern).matcher(clientdata).matches()) {
                    key = clientdata.split("set ")[1].split(" ")[0];
                    value = clientdata.split("set ")[1].split(" ")[1] + ":" + instream.readUTF();
                    hmap.put(key, value);

                    //synchronously writing key value pairs to the filesystem.
                    synchronized (this) {
                        fw.write(key + ":" + value + System.getProperty("line.separator"));
                        fw.flush();
                    }
                    outstream.writeUTF("STORED");

                } //GET 
                else if (Pattern.compile(getPattern).matcher(clientdata).matches()) {

                    key = clientdata.split("get ")[1];
                    value = hmap.get(key);
                    if (value != null) {
                        outstream.writeUTF("VALUE " + key + " " + value.split(":")[0]);
                        outstream.writeUTF(value.split(":")[1]);
                        outstream.writeUTF("END");
                    } else {
                        outstream.writeUTF("Key not found");
                    }

                } //closing client connection on logout
                else if (clientdata.equals("logout")) {
                    instream.close();
                    outstream.close();
                    s.close();
                    break;
                } else {
                    outstream.writeUTF("Invalid Input");
                }
            } //handling lost connection
            catch (IOException e) {
                System.out.println("Connection to client " + s + " lost.");
                break;
            }
        }
    }
}
