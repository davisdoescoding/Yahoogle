package cis5550.flame;

// package imports
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Loader;
import cis5550.tools.Logger;
import cis5550.webserver.Server;

// file management
import java.io.File;

// i/o
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;

//char manipulation
import java.net.URLDecoder;

//data structures
import java.util.HashMap;
import java.util.Vector;


class Coordinator extends cis5550.generic.Coordinator {
  // logger 4 debug/info logs
  private static final Logger logger = Logger.getLogger(Coordinator.class);

  // version info
  private static final String COORDINATOR_VERSION = "v1.5 Jan 1 2023";

  // id counter 4 jobs
  static int nextJobId = 1;

  // store job outputs
  static HashMap<String, String> jobOutputs;

  // kvs client instance
  public static KVSClient kvsClient;

  // default constructor
  Coordinator() {}

  public static void main(String[] args) {
    // chk num args, print usage on err
    if (args.length != 2) {
      System.err.println("Syntax: Coordinator <port> <kvsCoordinator>");
      System.exit(1);
    }

    // parse port num from args
    int serverPort = Integer.parseInt(args[0]);

    // init kvs client
    kvsClient = new KVSClient(args[1]);

    // init job outputs map
    jobOutputs = new HashMap<>();

    // log startup msg w/ version info
    logger.info("flame coordinator (" + COORDINATOR_VERSION + ") starting on port " + serverPort);

    // set server port
    Server.port(serverPort);

    // reg endpoint routes
    registerRoutes();

    // main page route (GET)
    Server.get("/", (request, response) -> {
      // return html 4 flame homepage
      response.type("text/html");
      return "<html><head><title>Flame coordinator</title></head><body><h3>Flame Coordinator</h3>\n"
              + clientTable() + "</body></html>";
    });

    // handle job submission (POST)
    Server.post("/submit", (request, response) -> {
      // get main class from params
      String mainClass = request.queryParams("class");
      logger.info("new job submitted -- main class is " + mainClass);

      // chk if class name is missing
      if (mainClass == null) {
        response.status(400, "Bad request");
        return "missing class name parameter";
      }

      // gather job args
      Vector<String> jobArgs = new Vector<>();
      for (int argIndex = 1; request.queryParams("arg" + argIndex) != null; argIndex++) {
        jobArgs.add(URLDecoder.decode(request.queryParams("arg" + argIndex), "UTF-8"));
      }

      // setup upload threads & results array
      Thread[] uploadThreads = new Thread[getWorkers().size()];
      final String[] uploadResults = new String[getWorkers().size()];

      // start worker upload threads
      for (int workerIndex = 0; workerIndex < getWorkers().size(); ++workerIndex) {
        String workerUrl = "http://" + getWorkers().elementAt(workerIndex) + "/useJAR";
        int finalWorkerIndex = workerIndex;

        uploadThreads[workerIndex] = new Thread("JAR upload #" + (finalWorkerIndex + 1)) {
          public void run() {
            // try to upload JAR to worker
            try {
              uploadResults[finalWorkerIndex] = new String(HTTP.doRequest("POST", workerUrl, request.bodyAsBytes()).body());
            } catch (Exception ex) {
              // catch upload err, log exception
              uploadResults[finalWorkerIndex] = "Exception: " + ex.getMessage();
              ex.printStackTrace();
            }
          }
        };
        uploadThreads[workerIndex].start();
      }

      // join all upload threads
      for (int threadIndex = 0; threadIndex < uploadThreads.length; threadIndex++) {
        try {
          uploadThreads[threadIndex].join();
          // log thread result
          logger.debug("jar upload #" + (threadIndex + 1) + ": " + uploadResults[threadIndex]);
        } catch (InterruptedException ignored) {}
      }

      // generate job ID
      int jobId = nextJobId++;
      String jarFileName = "job-" + jobId + ".jar";

      // save uploaded JAR file
      File jarFile = new File(jarFileName);
      try (FileOutputStream fileOutputStream = new FileOutputStream(jarFile)) {
        fileOutputStream.write(request.bodyAsBytes());
      }

      // rm prev outputs 4 this JAR
      jobOutputs.remove(jarFileName);

      // run job via loader
      try {
        Loader.invokeRunMethod(jarFile, mainClass, new FlameContextImpl(jarFileName), jobArgs);

      } catch (IllegalAccessException ex) {
        response.status(400, "Bad request");
        return "check class " + mainClass;

      } catch (NoSuchMethodException ex) {
        response.status(400, "Bad request");
        return "check class " + mainClass;

      } catch (InvocationTargetException ex) {
        // log and return job exception
        logger.error("the job threw exception:", ex.getCause());
        StringWriter stackTraceWriter = new StringWriter();
        ex.getCause().printStackTrace(new PrintWriter(stackTraceWriter));
        response.status(500, "Job threw an exception");
        return stackTraceWriter.toString();
      }

      // chk if output exists or return msg
      return jobOutputs.containsKey(jarFileName)
              ? jobOutputs.get(jarFileName)
              : "Job finished, but produced no output.";
    });

    // route 4 getting version info
    Server.get("/version", (request, response) -> {
      return COORDINATOR_VERSION;
    });
  }
}
