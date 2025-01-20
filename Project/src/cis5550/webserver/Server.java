package cis5550.webserver;

// i/o tools for handling i/o and processing
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

// networking utils for socket management
import java.net.ServerSocket;
import java.net.Socket;

// char encoding and crypto utils
import java.security.KeyStore;
import java.security.SecureRandom;
import java.net.URLDecoder;

// data structures
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

// concurrency utils for thread safety
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// secure socket layer utils
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

// logger utils
import cis5550.tools.Logger;

public class Server implements Runnable {

    private static final Logger log = Logger.getLogger(Server.class);

    //constant for worker threads
    private static final int NUM_WORKERS = 100;

    // static server properties
    static int port;
    static Map<String, String> staticFilePath;
    static boolean serverRunning = false;
    static Server serverInstance = null;


    static String currentHost = "default";

    // instance-specific server properties
    int securePort;
    Map<String, SessionImpl> sessions;
    Map<String, Vector<RouteEntry>> routes;
    BlockingQueue<Socket> blockingQueue;

    public static class staticFiles {
        public staticFiles() {
        }

        public static void location(String var0) {
            Server.launchIfNecessary();
            Server.serverInstance.setStaticLocation(var0);
        }
    }

    static void launchIfNecessary() {

        log.debug("checking if need to launch server ...");
        if (serverInstance == null) {
            log.debug("serverInstance null, launching new server...");
            serverInstance = new Server();
        }

        //update running flag
        if (!serverRunning) {
            serverRunning = true;

            //init new thread
            Thread serverThread = new Thread(serverInstance);
            serverThread.start();
            log.info("Server thread started.");
        }
    }

    public static void port(int serverPort) {

        log.info("setting server port to " + serverPort);

        //create new instance of not yet created
        if (serverInstance == null) {
            serverInstance = new Server();
        }

        //set port on
        serverInstance.setPort(serverPort);
    }

    protected SessionImpl createSession() {
        log.debug("creating new session...");
        //init new session instance
        SessionImpl newSession = new SessionImpl();

        //sync on sessions map to ensure thread safety in updates
        synchronized (this.sessions) {
            //add new session to sessions map with ID as key
            this.sessions.put(newSession.id(), newSession);
            //return new sessions
            return newSession;
        }
    }


    public static void securePort(int secureServerPort) {
        log.info("setting secure port to " + secureServerPort);
        //create new instance of server for secure
        if (serverInstance == null) {
            serverInstance = new Server();
        }

        serverInstance.setSecurePort(secureServerPort);
    }

    void setStaticLocation(String staticPath) {
        // log static file setup
        log.info("Serving static files for " + currentHost + " from " + staticPath);

        staticFilePath.put(currentHost, staticPath);
    }

    Server() {
        // default server settings
        port = 8000;
        serverRunning = false;
        staticFilePath = new HashMap<>();
        this.blockingQueue = new LinkedBlockingQueue<>();
        this.routes = new HashMap<>();
        this.routes.put("default", new Vector<>());
        this.sessions = new HashMap<>();
        this.securePort = -1;

        // log server initialization
        log.debug("Server initialized with default settings.");
    }

    void addRoute(String method, String path, Route handler) {
        // log route addition
        log.info("Adding route: " + method + " " + path);
        ((Vector<RouteEntry>) this.routes.get(currentHost)).add(new RouteEntry(method, path, handler));
    }

    public static void get(String path, Route handler) {
        // log GET route setup
        log.debug("Setting up GET route for " + path);

        launchIfNecessary();
        serverInstance.addRoute("GET", path, handler);
    }

    public static void put(String path, Route handler) {
        // log PUT route setup
        log.debug("Setting up PUT route for " + path);

        launchIfNecessary();
        serverInstance.addRoute("PUT", path, handler);
    }

    public static void post(String path, Route handler) {
        // log POST route setup
        log.debug("Setting up POST route for " + path);

        launchIfNecessary();
        serverInstance.addRoute("POST", path, handler);
    }

    public void setPort(int serverPort) {
        // check if server is running before setting port
        if (serverRunning) {
            throw new RuntimeException("call portt() before calling any other methods");
        } else {
            port = serverPort;
            log.info("server port set to " + serverPort);
        }
    }

    public void setSecurePort(int secureServerPort) {
        //check if server running before setting secure port
        if (serverRunning) {
            throw new RuntimeException("call securePort() before calling any other methods");
        } else {
            this.securePort = secureServerPort;
            log.info("secure port set to " + secureServerPort);
        }
    }

    boolean patternMatches(String routePattern, String requestPath, HashMap<String, String> routeParams) {
        log.debug("matching pattern: " + routePattern + " with path: " + requestPath);

        // split pattern and path into segments
        String[] patternSegments = routePattern.split("/");
        String[] pathSegments = requestPath.split("/");

        // check if seg counts match
        if (patternSegments.length != pathSegments.length) {
            return false;
        } else {

            // iter over segments to match path and pattern
            for (int i = 0; i < patternSegments.length; ++i) {
                if (patternSegments[i].startsWith(":")) {

                    // extract and decode param values
                    try {
                        routeParams.put(patternSegments[i].substring(1), URLDecoder.decode(pathSegments[i], "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        log.error("error decoding url parameter: " + e.getMessage());
                    }
                } else if (!patternSegments[i].equals(pathSegments[i])) {

                    //static segments don't match
                    return false;
                }
            }

            //all segments match
            return true;
        }
    }

    void parseQueryParams(String query, HashMap<String, String> queryParams) {
        log.debug("parsing query params: " + query);

        // split query string into k:v pairs
        String[] queryPairs = query.split("&");

        // iterate through each k:v pair
        for (String queryPair : queryPairs) {
            // split pair into k:v
            String[] keyValue = queryPair.split("=", 2);

            try {
                // decode value and add pair to queryParams map
                queryParams.put(keyValue[0], keyValue.length > 1 ? URLDecoder.decode(keyValue[1], "UTF-8") : "");
            } catch (UnsupportedEncodingException e) {
                log.error("error decoding query parameter: " + e.getMessage());
            }
        }
    }


    // method to send an error response to the client
    void sendError(PrintWriter responseWriter, int statusCode, String statusMessage) {
        log.debug("sending error response with status code: " + statusCode + " and message: " + statusMessage);

        //construct http error message
        responseWriter.write(
                "HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n" +  //status line
                        "Content-Length: " + (4 + statusMessage.length()) + "\r\n\r\n" + //headers
                        statusCode + " " + statusMessage //response body
        );
        log.debug("error response sent successfully");
    }

    // main server loop accepting connections
    void serverLoop(ServerSocket serverSocket) {
        while (true) {
            try {
                // accept new client connection
                Socket clientSocket = serverSocket.accept();
                // disable Nagle's algorithm
                clientSocket.setTcpNoDelay(true);
                // add socket to blocking queue
                this.blockingQueue.put(clientSocket);
            } catch (Exception e) {
                // log and handle exceptions
                e.printStackTrace();
            }
        }
    }

    public void run() {
        // init server socket for secure/not-secure
        ServerSocket httpSocket = null;
        ServerSocket httpsSocket = null;

        // keystore file name and pwd
        String keystoreFile = "keystore.jks";
        String keystorePassword = "secret";

        // set thread name
        Thread.currentThread().setName("main");
        log.info("webserver (v1.7 dec 31 2022) starting on port " + port);

        try {
            // create server socket for http
            httpSocket = new ServerSocket(port);

            // setup https if secure port enabled
            if (this.securePort >= 0) {

                // load keystore
                KeyStore keystore = KeyStore.getInstance("JKS");
                keystore.load(new FileInputStream(keystoreFile), keystorePassword.toCharArray());

                // init key manager factory
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                keyManagerFactory.init(keystore, keystorePassword.toCharArray());

                // setup ssl context
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory.getKeyManagers(), (TrustManager[]) null, (SecureRandom) null);

                // create secure server socket
                httpsSocket = sslContext.getServerSocketFactory().createServerSocket(this.securePort);
            }
        } catch (Exception startupException) {
            //fatal startup error - exit
            startupException.printStackTrace();
            System.exit(1);
        }

        // mark server running
        serverRunning = true;

        // init worker threads
        for (int workerIndex = 0; workerIndex < NUM_WORKERS; ++workerIndex) {
            int finalWorkerIndex = workerIndex;
            new Thread("W-" + (finalWorkerIndex < 10 ? "0" : "") + finalWorkerIndex) {
                public void run() {
                    // start worker thread
//                    this.workerThread(finalWorkerIndex);
                    while (true) {
                        try {
                            // get client socket from queue
                            Socket clientSocket = blockingQueue.take();
                            // create and start a new ThreadHandler
                            new Thread(new ConnectionHandler(clientSocket, Server.this)).start();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();
        }

        // start session exp thread
        new Thread("S-XP") {
            public void run() {
                while (true) {
                    try {

                        //@TODO
                        Thread.sleep(1000L);
                    } catch (InterruptedException ignoredException) {
                    }

                    synchronized (Server.this.sessions) {
                        // iter through sessions and rm expired ones
                        Iterator<String> sessionIterator = Server.this.sessions.keySet().iterator();
                        while (sessionIterator.hasNext()) {
                            String sessionId = sessionIterator.next();
                            SessionImpl session = (SessionImpl) Server.this.sessions.get(sessionId);
                            if (session.lastAccessedTime() < System.currentTimeMillis() - (long) (1000 * session.maxActiveInterval())) {
                                Server.this.sessions.remove(sessionId);
                            }
                        }
                    }
                }
            }
        }.start();

        // start https server loop if secure port enabled
        if (this.securePort >= 0) {
            ServerSocket finalHttpsSocket = httpsSocket;
            new Thread("TTLS") {
                public void run() {

                    // start server loop for https
                    Server.this.serverLoop(finalHttpsSocket);
                }
            }.start();
        }

        // start http server loop
        this.serverLoop(httpSocket);
    }

    class RouteEntry {
        // http method, path, route handler
        String method;
        String path;
        Route route;

        RouteEntry(String routeMethod, String routePath, Route routeHandler) {
            this.method = routeMethod;
            this.path = routePath;
            this.route = routeHandler;
        }
    }
}
