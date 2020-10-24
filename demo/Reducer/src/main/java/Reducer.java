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

            System.out.println("******** Data in reducer: " + this.data);


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
//                        try (RandomAccessFile reader = new RandomAccessFile(files, "rw");
////                             FileLock lock = reader.getChannel().lock()) {
//                        ){
                           //if function is WORD_COUNT, append the count to the key

                            try {
//                                reader.seek(reader.length());
                                bufferedWriter.write((tokens[0] + "," + total + "\n"));
//                                reader.write((tokens[0] + "," + total + "\n").getBytes());
                            } catch (IOException e) {
                                logger.severe("CANNOT WRITE TO THE FILE");
                            }


//                        } catch (FileNotFoundException ex) {
//                            ex.printStackTrace();
//                        } catch (IOException ex) {
//                            ex.printStackTrace();
//                        }
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
                                System.out.println("filename " + file + " count " + count);
                            }


                            File files=new File("final_data"+noreducers+".csv");
                            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(files.getCanonicalFile(), true));
//                            try (RandomAccessFile reader = new RandomAccessFile(files, "rw");
////                                 FileLock lock = reader.getChannel().lock()) {
//                            ){

                                //if function is INVERTED_INDEX, then append file name and word count

                                try {
                                    System.out.println("Inside Writer"+sb.toString());
//                                    reader.seek(reader.length());
                                    bufferedWriter.write((sb.toString() + "\n"));
                                   // reader.write((sb.toString() + "\n").getBytes());
                                } catch (IOException e) {
                                    System.out.println("CANNOT WRITE TO THE FILE");
                                }


//                            } catch (FileNotFoundException ex) {
//                                ex.printStackTrace();
//                            } catch (IOException ex) {
//                                ex.printStackTrace();
//                            }
                            bufferedWriter.close();
                        }


//                    String key = tokens[0];
//                    String files = tokens[1];
//                    String value = tokens[2];
//                    StringBuilder sb = new StringBuilder();


//                    for(String file : files)
//                    {
//                        String[] values = file.split(" ");
//                        String[] count = values[1].split(",");
//                        int total = 0;
//                        for(String c : count)
//                        {
//                            if(c.trim().equals("1"))
//                                total++;
//                        }
//                        sb.append(values[0]);
//                        sb.append(" ");
//                        sb.append(total+" ");
//                    }
//                    map2.put(tokens[0], sb.toString());
//                }

                    }

                    //Write the final data into a file
//                try (RandomAccessFile reader = new RandomAccessFile(new File("final_data.csv"), "rw");
//                     FileLock lock = reader.getChannel().lock()) {
//
//                    if (function.equals("WORD_COUNT")) {
//                        //if function is WORD_COUNT, append the count to the key
//                        map.forEach((key, value) -> {
//                            try {
//                                reader.seek(reader.length());
//                                reader.write((key + "," + value + "\n").getBytes());
//                            } catch (IOException e) {
//                                logger.severe("CANNOT WRITE TO THE FILE");
//                            }
//                        });
//                    } else {
//                        //if function is INVERTED_INDEX, then append file name and word count
//                        map2.forEach((key, value) -> {
//                            try {
//                                reader.seek(reader.length());
//                                reader.write((key + "," + value + "\n").getBytes());
//                            } catch (IOException e) {
//                                System.out.println("CANNOT WRITE TO THE FILE");
//                            }
//                        });
//                    }

//            } catch (FileNotFoundException e) {
//                logger.severe(String.valueOf(e.getStackTrace()));
//            }



//                } catch (IOException e) {
//                    logger.severe(String.valueOf(e.getStackTrace()));
//                }
                }
//        }

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