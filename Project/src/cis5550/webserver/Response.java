package cis5550.webserver;

public interface Response {


    //set body as string or byte array
    void body(String body);
    void bodyAsBytes(byte bodyArg[]);

    //adds header with name attribute and val
    void header(String name, String value);
    void type(String contentType);

    //set status code and reason phrase
    void status(int statusCode, String reasonPhrase);

    //directly write out bytes to stream without buffering for explicit response dispatch
    void write(byte[] b) throws Exception;


    void redirect(String url, int responseCode);

    void halt(int statusCode, String reasonPhrase);
};