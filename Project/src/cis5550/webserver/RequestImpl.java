package cis5550.webserver;

// Logging utilities
import cis5550.tools.Logger;

// Networking
import java.net.InetSocketAddress;

// Character encoding
import java.nio.charset.StandardCharsets;

// Collections and data structures
import java.util.Map;
import java.util.Set;


// implementation of the Request interface
class RequestImpl implements Request {

    private static final Logger log = Logger.getLogger(RequestImpl.class);

    // Request metadata
    String httpMethod;                // http method
    String requestUrl;                // req URL
    String httpProtocol;              // http protocol version
    InetSocketAddress clientAddress;  // ip address of client

    // Request headers and parameters
    Map<String, String> requestHeaders;    // http headers in request
    Map<String, String> queryParameters;   // query string params
    Map<String, String> routeParameters;   // extract params from route matching

    // Session management
    SessionImpl activeSession;             // active session for the req
    boolean isCookiePresent;               // whether session cookie present

    // Request body
    byte[] rawRequestBody;                 // raw body of req

    // Server reference
    Server serverInstance;                 // ref to server handling this request


    // constructor - builds a request obj
    RequestImpl(String method, String url, String protocol, Map<String, String> headers,
                Map<String, String> queryParams, Map<String, String> params,
                InetSocketAddress address, byte[] body, Server server) {
        this.httpMethod = method; // store http method (only GET, POST, HEAD, PUT.)
        this.requestUrl = url; // store req url
        this.clientAddress = address; // store client addr (IP + port)
        this.httpProtocol = protocol; // store protocol (HTTP/1.1, etc.)
        this.requestHeaders = headers; // store headers
        this.queryParameters = queryParams; // store query params (?key=value)
        this.routeParameters = params; // store route params (:param in route)
        this.rawRequestBody = body; // store raw req body
        this.serverInstance = server; // reference to the server instance
        this.activeSession = null; // no session yet
        this.isCookiePresent = false; // no cookie detected yet

        // check for cookies in the request
        if (this.requestHeaders.get("cookie") != null) {
            String[] cookieArray = this.requestHeaders.get("cookie").split(";");

            for (String cookie : cookieArray) {
                String[] cookieParts = cookie.trim().split("=");

                // check if SessionID cookie exists
                if (cookieParts[0].equals("SessionID")) {
                    synchronized (this.serverInstance.sessions) {
                        // lookup session by ID
                        this.activeSession = this.serverInstance.sessions.get(cookieParts[1]);
                    }

                    // invalidate expired sessions
                    if (this.activeSession != null
                            && this.activeSession.lastAccessedTime() < System.currentTimeMillis()
                            - (this.activeSession.maxActiveInterval() * 1000L)) {
                        log.warn("Session expired: " + cookieParts[1]);
                        this.activeSession = null;
                    }

                    // mark cookie as valid if session exists
                    if (this.activeSession != null) {
                        this.isCookiePresent = true;
                        this.activeSession.lastAccessedTime(); // update access time
                        log.info("Valid session found for SessionID: " + cookieParts[1]);
                    }
                }
            }
        }
    }

    // return HTTP method of the request
    public String requestMethod() {
        return this.httpMethod;
    }

    // update route params dynamically
    public void setParams(Map<String, String> params) {
        this.routeParameters = params;
    }

    // return client port number
    @Override
    public int port() {
        return this.clientAddress.getPort();
    }

    // return requested URL
    @Override
    public String url() {
        return this.requestUrl;
    }

    // return HTTP protocol version
    @Override
    public String protocol() {
        return this.httpProtocol;
    }

    // return content-type header
    @Override
    public String contentType() {
        return this.requestHeaders.get("content-type");
    }

    // return client IP address
    @Override
    public String ip() {
        return this.clientAddress.getAddress().getHostAddress();
    }

    // return req body as string (decoded)
    @Override
    public String body() {
        return new String(this.rawRequestBody, StandardCharsets.UTF_8);
    }

    // return req body as raw bytes
    @Override
    public byte[] bodyAsBytes() {
        return this.rawRequestBody;
    }

    // return content length (size of body)
    @Override
    public int contentLength() {
        return this.rawRequestBody.length;
    }

    // fetch specific header by name
    @Override
    public String headers(String name) {
        return this.requestHeaders.get(name.toLowerCase());
    }

    // return all header names
    @Override
    public Set<String> headers() {
        return this.requestHeaders.keySet();
    }

    // fetch specific query param by name
    @Override
    public String queryParams(String name) {
        return this.queryParameters.get(name);
    }

    // return all query param names
    @Override
    public Set<String> queryParams() {
        return this.queryParameters.keySet();
    }

    // fetch specific route param by name
    @Override
    public String params(String name) {
        return this.routeParameters.get(name);
    }

    // return all route params
    @Override
    public Map<String, String> params() {
        return this.routeParameters;
    }

    // fetch or create session
    @Override
    public Session session() {
        if (this.activeSession == null) {
            this.activeSession = this.serverInstance.createSession(); // create session if none
            log.info("new session created: " + this.activeSession.id());
        }
        return this.activeSession;
    }
}
