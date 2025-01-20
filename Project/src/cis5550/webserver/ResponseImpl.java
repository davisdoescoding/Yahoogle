package cis5550.webserver;

//i/o utils
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

//data structures
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


class ResponseImpl implements Response {

    // default status code
    int statusCode = 200;
    // default status message
    String statusMessage = "OK";

    // rsp body as byte array
    byte[] responseBody = null;
    // headers for the response
    HashMap<String, List<String>> responseHeaders;


    // output stream to write the response
    OutputStream outputStream;


    // flag to check if response is already committed
    boolean isCommitted = false;
    // flag for HEAD requests
    boolean isHeadRequest;

    // init response object
    ResponseImpl(OutputStream outputStream, boolean isHeadRequest) {
        this.outputStream = outputStream;
        this.responseHeaders = new HashMap<>();
        // add default content-type
        this.header("content-type", "text/html");
        this.isHeadRequest = isHeadRequest;
    }

    // set status code + message
    public void status(int code, String message) {
        this.statusCode = code;
        this.statusMessage = message;
    }

    // add header to response
    public void header(String headerName, String headerValue) {
        List<String> headerValues = this.responseHeaders.get(headerName);
        if (headerValues == null) {
            // create new list if header doesn't exist
            headerValues = new LinkedList<>();
            this.responseHeaders.put(headerName, headerValues);
        }
        headerValues.add(headerValue); // append val to header
    }

    // set body as bytes
    public void bodyAsBytes(byte[] bodyContent) {
        this.responseBody = bodyContent;
    }

    // set body as str
    public void body(String bodyContent) {
        // convert string body to bytes
        this.responseBody = bodyContent == null ? null : bodyContent.getBytes();
    }

    // set content-type header
    public void type(String contentType) {
        // rm any existing content-type
        this.responseHeaders.remove("content-type");
        // add new content-type
        this.header("content-type", contentType);
    }

    // write raw data to output stream
    public void write(byte[] data) throws Exception {
        if (!this.isCommitted) {
            // commit rsp headers first
            this.commit();
        }
        if (!this.isHeadRequest) {
            // only write for non-HEAD reqs
            this.outputStream.write(data);

            //flush data
            this.outputStream.flush();
        }
    }

    // finalize headers and send to client
    void commit() throws Exception {

        if (this.isCommitted) {
            // can't commit twice, throw err
            throw new Exception("Response already committed!");
        } else {

            // write headers to stream
            PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(this.outputStream)));
            writer.write("HTTP/1.1 " + this.statusCode + " " + this.statusMessage + "\r\n");

            // iter over headers
            for (String headerName : this.responseHeaders.keySet()) {
                List<String> headerValues = this.responseHeaders.get(headerName);
                for (String headerValue : headerValues) {

                    // write each header and field value
                    writer.write(headerName + ": " + headerValue + "\r\n");
                }
            }

            // end of headers
            writer.write("\r\n");

            //send and mark sent
            writer.flush();
            this.isCommitted = true;
        }
    }

    // redir rsp to new location
    public void redirect(String location, int code) {

        // set status and location
        this.status(code, "Redirect");
        this.header("location", location);

        try {
            this.commit(); // finalize response
        } catch (Exception ignored) {
            // log err silently
        }
    }

    // stop any processing and send halt rsp
    public void halt(int code, String message) {
        // set status code and message
        this.status(code, message);

        try {
            //send halt rsp
            this.commit();
        } catch (Exception ignored) {

            // suppress any errs
        }
    }
}
