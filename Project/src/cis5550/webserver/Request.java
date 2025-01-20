package cis5550.webserver;

//data structures for managing request parsing
import java.util.Map;
import java.util.Set;

//inbound HTTP request objects follow standard interface
public interface Request {

    //return client IP address
    String ip();

    //return client port
    int port();


    /*
     * return request line elements
     *      GET /index.html HTTP/1.1
     *      requestMethod --> 'GET'
     *      url() --> '/index.html'
     *      protocol() --> 'HTTP/1.1'
     */
    String requestMethod();
    String url();
    String protocol();


    //return set of header fields client sent
    Set<String> headers();

    //helpers
    String headers(String name);
    String contentType();

    //access message body of request as string or byte array
    String body();
    byte[] bodyAsBytes();

    //num bytes
    int contentLength();

    //access query params in url target or req body
    Set<String> queryParams();
    String queryParams(String param);

    //access named params
    Map<String, String> params();
    String params(String name);

    //lookup current session or create new session object
    Session session();
};