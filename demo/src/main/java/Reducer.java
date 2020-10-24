import java.io.*;
import java.net.Socket;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Reducer extends Thread {
    Map<String, Integer> map = new ConcurrentHashMap<>();
    Map<String, String> map2 = new ConcurrentHashMap<>();
    String data;
    Logger logger;
    static Socket reducerSocket;
    int port;
    static Scanner reducer_input;
    static PrintStream reducer_output;
    String function;
    static int noreducers;

    public Reducer(Logger logger, int port, String function, int noreducers) {
        this.logger = logger;
        this.port = port;
        this.function = function;
        this.noreducers=noreducers;
    }

    public void run() {

        try {
            reducerSocket = new Socket("127.0.0.1", port);
            reducer_input = new Scanner(reducerSocket.getInputStream());
            reducer_output = new PrintStream(reducerSocket.getOutputStream());
            logger.info("Reducer started at port " + port);

            String data1 = "";

            //Accept the input data from the combiner
            while (reducer_input.hasNextLine()) {
                data1 = reducer_input.nextLine();
                break;
            }
            this.data = data1;


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
                                String file = values[j];
                                String[] data = values[j + 1].split(",");
                                int count = 0;
                                for (String v : data) {
                                    if (v.trim().equals("1"))
                                        count++;
                                }
                                sb.append(file + ",");
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

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        reducerSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

        } catch (IOException e) {
            logger.severe(String.valueOf(e.getStackTrace()));
        }
    }
}