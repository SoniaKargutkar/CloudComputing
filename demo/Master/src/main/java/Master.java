import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class  Master
{
    static private int port;
    static private String ip_address;
    static private int no_mappers;
    static private int no_reducers;
    static private String function;
    static private String filePath_config;
    static  private int[] mappers;
    static  private int[] reducers;
    static String[] dataset;
    static private String mapper_ip_address;
    static  private  String reducer_ip_address;
    static Map<String, Integer> map = new HashMap<>();
    static BufferedReader br;
    static FileHandler fh;
    static List<String> messages = new ArrayList<>();
    static private int kv_port;

    private final static Logger logger = Logger.getLogger(Master.class.getName());
    String filepath="master_config.properties";

    File configFile = new File(filepath);

    String mapperfilepath="mapper_config.properties";
    File mapperconfigFile = new File(mapperfilepath);

    String reducerconfigpath="reducer_config.properties";
    File reducerconfigFile = new File(reducerconfigpath);

    public static void main(String[] args) throws IOException, InterruptedException
    {
        //File Handler and formatting for logger
        fh = new FileHandler("LogFile.log", true);
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        System.setProperty("java.util.logging.SimpleFormatter.format","[%1$tF %1$tT] [%4$s]   %2$s %5$s%6$s%n");
        fh.setFormatter(formatter);

        Master master = new Master();
        master.readConfigFile();
        logger.info("Master started at port "+port);


        System.out.println("master kv port "+kv_port);
        Server server=new Server(kv_port,function);
        server.start();
//        Server server = new Server(kv_port, function);
//        server.start();

        //Read the total lines in input data and split amongst the mappers
        int dataset_count = master.getDatasetCount();
        master.readMapperConfig();
        int mapper_count = dataset_count%2 == 0 ? dataset_count/no_mappers : (dataset_count/no_mappers)+1;
        master.readInputData(mapper_count);

        ServerSocket[] serverSocket = new ServerSocket[no_mappers+1];
        Socket[] socket_mapper = new Socket[no_mappers+1];
        Scanner[] mapper_input = new Scanner[no_mappers+1];
        PrintStream[] mapper_output = new PrintStream[no_mappers+1];

        //Starting each mapper on a different port
        for(int i=1; i<=no_mappers; i++)
        {
            //Master(Server) Listener thread that listens to the mapper
            serverSocket[i] = new ServerSocket(mappers[i]);
            Socket socket = new Socket(ip_address, mappers[i]);
            socket_mapper[i] = serverSocket[i].accept();

            mapper_input[i] = new Scanner(socket_mapper[i].getInputStream());
            mapper_output[i] = new PrintStream(socket_mapper[i].getOutputStream());

            Mapper mapper = new Mapper( mappers[i], logger,function);
            mapper.start();
            logger.info("Assigning mapper ["+i+"] at port "+mappers[i]);
            Listener mapperListener = new Listener(serverSocket[i], messages, dataset[i-1]);
            mapperListener.start();
            mapper_output[i].println(dataset[i-1]);
            Thread.sleep(1000);
        }

        //Call the combiner when all the mappers have finished executing
        if(messages.size()==no_mappers)
        {
            logger.info("Task finished by Mappers");
            Combiner combiner = new Combiner(no_reducers, logger, function,kv_port);
            Thread.sleep(1000);
            String[] reduced_dataset = combiner.combiner();

            //Read the reducer configuration file
            master.readReducerConfig();
            ServerSocket[] reducerserverSocket = new ServerSocket[no_reducers + 1];
            Socket[] socket_reducer = new Socket[no_reducers + 1];
            Scanner[] reducer_input = new Scanner[no_reducers + 1];
            PrintStream[] reducer_output = new PrintStream[no_reducers + 1];

            //Assigning each reducer to a new port
            for (int i = 1; i <= no_reducers; i++) {
                reducerserverSocket[i] = new ServerSocket(reducers[i]);
                Socket socket1 = new Socket(ip_address, reducers[i]);
                socket_reducer[i] = reducerserverSocket[i].accept();

                reducer_input[i] = new Scanner(socket_reducer[i].getInputStream());
                reducer_output[i] = new PrintStream(socket_reducer[i].getOutputStream());

                Reducer reducer = new Reducer(logger, reducers[i], function,i);
                reducer.start();
                logger.info("Assigning reducer ["+i+"] at port " + reducers[i]);
                Listener reducerListener = new Listener(reducerserverSocket[i],messages,reduced_dataset[i-1]);
                reducerListener.start();
                Thread.sleep(1000);
            }
        }

        fh.close();

        System.out.println("In master "+messages.size());
        //If all the reducers have completed the task then exit
        if(messages.size() == (no_reducers+no_mappers))
        {
            logger.info("Task finished by Reducers");
            System.exit(0);
        }
    }

    //Read the master configuration file
    public void readConfigFile() throws IOException
    {
        Properties properties = new Properties();
        BufferedReader fs= new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/master_config.properties")));
        properties.load(fs);

        port = Integer.parseInt(properties.getProperty("PORT"));
        ip_address = properties.getProperty("IP-ADDRESS");
        no_mappers = Integer.parseInt(properties.getProperty("NO_MAPPER"));
        no_reducers = Integer.parseInt(properties.getProperty("NO_REDUCER"));
        function = properties.getProperty("FUNCTION");
        filePath_config = properties.getProperty("FILE_PATH");
        kv_port = Integer.parseInt(properties.getProperty("KV_PORT"));
        fs.close();
    }

    //Count the total numbers of lines in input data
    public int getDatasetCount() throws IOException
    {
        String[] filePaths = filePath_config.split(",");
        int count = 0;
        String str = "";
        for(String path : filePaths)
        {
        	BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/"+path)));
        	while((str = bufferedReader.readLine()) != null)
            {
                count++;
            }
        }
        return count;
    }

    //Read mapper configuration file
    public void readMapperConfig() throws IOException
    {
        mappers = new int[no_mappers+1];
        Properties properties = new Properties();
        BufferedReader fs= new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/mapper_config.properties")));
        properties.load(fs);

        for(int i=1; i<=no_mappers; i++)
        {
            mappers[i] = Integer.parseInt(properties.getProperty("MAPPER_"+i+"_PORT"));
        }
        mapper_ip_address = properties.getProperty("MAPPER_IP_ADDRESS");
        fs.close();
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
        reducer_ip_address = properties.getProperty("REDUCER_IP_ADDRESS");

        fs.close();
    }

    //Divide the input dataset amongst the reducers
    public void readInputData(int count) throws IOException
    {
        String[] filePaths = filePath_config.split(",");
        dataset = new String[no_mappers];
        int counter = 0;
        int i=0;
        String str = "";
        StringBuilder sb = new StringBuilder();

        for(int j=0; j<filePaths.length; j++)
        {
           BufferedReader bufferedReader= new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/"+filePaths[j])));

            while((str = bufferedReader.readLine()) != null)
            {
                str = str.toLowerCase();
                sb.append(str);
                sb.append("###"+(filePaths[j]));
                sb.append(":::");
                counter++;
                if(counter == count && i+1 != no_mappers) {
                    dataset[i] = sb.toString();
                    sb = new StringBuilder();
                    i++;
                    counter = 0;
                }
            }
            dataset[i] = sb.toString();
            bufferedReader.close();
        }
    }
}
