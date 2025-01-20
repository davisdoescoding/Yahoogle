package cis5550.webserver;

// imports for i/o handling
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

// imports for networking
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.Socket;

// imports for char encoding
import java.nio.charset.StandardCharsets;

// imports for data structures
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

// logger import
import cis5550.tools.Logger;


public class ConnectionHandler implements Runnable {

    // client socket for connection
    private final Socket clientSocket;
    // server instance for shared access
    private final Server server;

    // init connection handler with socket and server ref
    public ConnectionHandler(Socket clientSocket, Server server) {
        this.clientSocket = clientSocket;
        this.server = server;
    }

    // logger for connection handler
    private static final Logger log = Logger.getLogger(ConnectionHandler.class);

    @Override
    public void run() {

        try {
            // get input stream from client
            InputStream clientInputStream = clientSocket.getInputStream();
            // init buffer for client data
            byte[] requestBuffer = new byte[100000];
            // track offset in buffer
            int bufferOffset = 0;

            PrintWriter responseWriter;
            for (boolean requestComplete = false; !requestComplete; responseWriter.flush()) {
                // collect req body
                ByteArrayOutputStream requestBodyStream = new ByteArrayOutputStream();
                boolean parsingHeaders = true;
                // http method, req path, version
                String httpMethod = null;
                String requestPath = null;
                String httpVersion = null;

                // map for req headers
                HashMap requestHeaders = new HashMap();
                boolean badRequest = false;

                //len req body
                int contentLength = -1;

                // temp var for header keys
                String headerKey;

                do {
                    // read from client input stream
                    int numBytesRead = clientInputStream.read(requestBuffer, bufferOffset, requestBuffer.length - bufferOffset);
                    // check for end of stream or full buffer
                    if (numBytesRead < 0 || bufferOffset >= requestBuffer.length) {
                        requestComplete = true;
                        break;
                    }

                    // update buffer offset
                    bufferOffset += numBytesRead;
                    // count consec newline characters
                    int numNewLineChars;

                    if (parsingHeaders) {
                        // reset newline counter
                        numNewLineChars = 0;

                        // parsing headers loop
                        breakHeaderParse:
                        for (int bufferIndex = 0; bufferIndex < bufferOffset; ++bufferIndex) {
                            // check for newline character
                            if (requestBuffer[bufferIndex] == 10) {
                                ++numNewLineChars;
                            }
                            // reset if not carriage return
                            else if (requestBuffer[bufferIndex] != 13) {
                                numNewLineChars = 0;
                            }

                            // end of headers detected
                            if (numNewLineChars == 2) {
                                // write headers to stream
                                requestBodyStream.write(requestBuffer, 0, bufferIndex);
                                ByteArrayInputStream headerInputStream = new ByteArrayInputStream(requestBodyStream.toByteArray());

                                // header readers
                                BufferedReader headerBufferReader = new BufferedReader(new InputStreamReader(headerInputStream));

                                // parse request line
                                String[] requestLineParts = headerBufferReader.readLine().split(" ");

                                //check request line valid
                                if (requestLineParts.length == 3) {
                                    // extract http method
                                    httpMethod = requestLineParts[0];
                                    // extract request path
                                    requestPath = requestLineParts[1];
                                    // extract http version
                                    httpVersion = requestLineParts[2];
                                }

                                // invalid request
                                else {
                                    badRequest = true;
                                }


                                while (true) {

                                    // read single line from header buff
                                    String singleLine = headerBufferReader.readLine();

                                    // check if line is empty for end of headers
                                    if (singleLine.equals("")) {

                                        // check for content-len header and parse it
                                        if (requestHeaders.get("content-length") != null) {
                                            contentLength = Integer.parseInt((String) requestHeaders.get("content-length"));
                                        }

                                        // reset req body stream
                                        requestBodyStream.reset();
                                        // shift buff to rm processed header
                                        System.arraycopy(requestBuffer, bufferIndex + 1, requestBuffer, 0, bufferOffset - (bufferIndex + 1));
                                        // update buff offset
                                        bufferOffset -= bufferIndex + 1;

                                        // switch out of header parsing mode
                                        parsingHeaders = false;

                                        // break out this loop
                                        break breakHeaderParse;
                                    }

                                    // split header line into k:v parts
                                    String[] headerParts = singleLine.split(" ", 2);

                                    // validate header parts
                                    if (headerParts.length == 2) {
                                        // extract header k:v and store in map
                                        headerKey = headerParts[0].substring(0, headerParts[0].length() - 1);
                                        requestHeaders.put(headerKey.toLowerCase(), headerParts[1]);
                                    }

                                    // mark req as bad if header invalid
                                    else {
                                        badRequest = true;
                                    }
                                }
                            }
                        }
                    }


                    if (!parsingHeaders) {
                        // determine num bytes to write based on content len or buffer offset
                        numNewLineChars = contentLength >= 0 && bufferOffset > contentLength ? contentLength : bufferOffset;
                        // write body data to req body stream
                        requestBodyStream.write(requestBuffer, 0, numNewLineChars);
                        // shift remaining buff content after writing
                        System.arraycopy(requestBuffer, numNewLineChars, requestBuffer, 0, bufferOffset - numNewLineChars);
                        // update buff offset after processing
                        bufferOffset -= numNewLineChars;
                    }

                    // continue loop while parsing headers or if body size less than content len
                } while (parsingHeaders || contentLength >= 0 && requestBodyStream.size() < contentLength);

                //done
                if (requestComplete) {
                    break;
                }

                // create rsp writer for sending client responses
                responseWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream())), true);
                boolean responseSent = false;

                //@TODO: what to set hostname as for final project?
                String hostName = "default";

                // extract host name from headers if present
                if (requestHeaders.get("host") != null) {
                    hostName = ((String) requestHeaders.get("host")).split(":")[0];
                }

                // fallback to default host name if no matching route exists
                if (server.routes.get(hostName) == null) {
                    hostName = "default";
                }

                // log received http request details
                log.info("received a " + httpMethod + " req for " + requestPath + " on host '" + hostName + "");

                // check for missing or null req fields
                if (httpMethod == null || requestPath == null || httpVersion == null || hostName == null) {
                    server.sendError(responseWriter, 400, "bad request");
                    responseSent = true;
                }

                // check http version is supported
                if (!responseSent && !httpVersion.equals("HTTP/1.1") && !httpVersion.equals("HTTP/1.0")) {
                    server.sendError(responseWriter, 505, "version not supported");
                    responseSent = true;
                }

                // validate only basic http methods
                if (!responseSent && !httpMethod.equals("GET") && !httpMethod.equals("HEAD") && !httpMethod.equals("PUT") && !httpMethod.equals("POST")) {
                    server.sendError(responseWriter, 501, "not implemented");
                    responseSent = true;
                }

                // check for any attempt to traverse dir in req path
                if (!responseSent && requestPath.contains("..")) {
                    server.sendError(responseWriter, 403, "forbidden");
                    responseSent = true;
                }

                // init query params map and sanitize req path
                HashMap queryParams = new HashMap();
                String sanitizedPath = requestPath;

                // parse query params if not yet responded
                if (!responseSent) {

                    // check for query params in path
                    if (requestPath.indexOf(63) >= 0) {
                        server.parseQueryParams(requestPath.substring(requestPath.indexOf(63) + 1), queryParams);
                        sanitizedPath = requestPath.substring(0, requestPath.indexOf(63));
                    }

                    // parse urlencoded form data if present
                    if (requestHeaders.get("content-type") != null &&
                            ((String) requestHeaders.get("content-type")).equals("application/x-www-form-urlencoded") &&
                            requestBodyStream.size() > 0) {
                        server.parseQueryParams(new String(requestBodyStream.toByteArray(), StandardCharsets.UTF_8), queryParams);
                    }
                }

                // create req and rsp objects
                RequestImpl RequestObj = new RequestImpl(httpMethod, sanitizedPath, httpVersion, requestHeaders, queryParams, (Map) null,
                        (InetSocketAddress) clientSocket.getRemoteSocketAddress(), requestBodyStream.toByteArray(), server);
                ResponseImpl ResponseObj = new ResponseImpl(clientSocket.getOutputStream(), httpMethod != null ? httpMethod.equals("HEAD") : false);

                // set default rsp headers
                ResponseObj.header("Connection", "close");


                // iter through routes for curr host
                for (int routeIndex = 0; !responseSent && routeIndex < ((Vector) server.routes.get(hostName)).size(); ++routeIndex) {
                    // get curr route
                    Server.RouteEntry currRoute = (Server.RouteEntry) ((Vector) server.routes.get(hostName)).elementAt(routeIndex);
                    // init map for route params
                    HashMap routeParams = new HashMap();

                    // check if route matches req method and path
                    if ((currRoute.method.equals(httpMethod) || httpMethod.equals("HEAD") && currRoute.method.equals("GET")) &&
                            server.patternMatches(currRoute.path, sanitizedPath, routeParams)) {

                        // set params in request object
                        RequestObj.setParams(routeParams);
                        // init route response object
                        Object RouteRspObj = null;

                        try {
                            // handle route and get rsp
                            RouteRspObj = currRoute.route.handle(RequestObj, ResponseObj);

                            // check if rsp not yet committed
                            if (!ResponseObj.isCommitted) {
                                // set rsp body if not null
                                if (RouteRspObj != null) {
                                    ResponseObj.body(RouteRspObj.toString());
                                }

                                // set content-length header
                                int responseBodyLength = ResponseObj.responseBody != null ? ResponseObj.responseBody.length : 0;
                                ResponseObj.header("Content-Length", "" + responseBodyLength);

                                // handle session cookies
                                SessionImpl activeSession = RequestObj.activeSession;
                                if (activeSession != null) {
                                    if (activeSession.lastAccessedTime() > 0L) {
                                        if (!RequestObj.isCookiePresent) {
                                            ResponseObj.header("Set-Cookie", "SessionID=" + activeSession.id());
                                        }
                                    } else {
                                        ResponseObj.header("Set-Cookie", "SessionID=x; Max-Age=0" + activeSession.id());
                                    }
                                }

                                // commit response
                                int responseStatusCode = ResponseObj.statusCode;
                                log.debug("sending a " + responseStatusCode + " response " + (ResponseObj.responseBody == null ? "with an empty body" : "(" + ResponseObj.responseBody.length + " bytes)"));
                                ResponseObj.commit();

                                // write rsp body to client if not null
                                if (ResponseObj.responseBody != null) {
                                    clientSocket.getOutputStream().write(ResponseObj.responseBody, 0, ResponseObj.responseBody.length);
                                }
                            } else {
                                // mark req as complete if response committed
                                requestComplete = true;
                            }

                            // flush output stream
                            clientSocket.getOutputStream().flush();

                            //exception thrown by route handling
                        } catch (Exception e) {
                            log.error("handler for " + currRoute.method + " route " + currRoute.path + " threw an exception:", e);

                            // send internal server err if rsp not committed
                            if (!ResponseObj.isCommitted) {
                                server.sendError(responseWriter, 500, "internal server error");
                            } else {
                                //otherwise, mark complete
                                requestComplete = true;
                            }
                        }

                        // mark rsp sent
                        responseSent = true;
                    }
                }

                // check if static file path exists for host
                if (!responseSent && server.staticFilePath.get(hostName) != null) {

                    // get static file obj for req path
                    File StaticFileObj = new File((String) server.staticFilePath.get(hostName), requestPath);

                    // check if file exists
                    if (StaticFileObj.exists()) {
                        // check if file can be read
                        if (StaticFileObj.canRead()) {
                            // check if method allowed
                            if (!httpMethod.equals("GET") && !httpMethod.equals("HEAD")) {
                                server.sendError(responseWriter, 405, "method not allowed");
                                responseSent = true;
                            } else {
                                // log returning static file
                                log.info("returning static file " + StaticFileObj.getAbsolutePath());

                                // determine content type based on file extension
                                headerKey = "application/octet-stream";

                                //default application, accept jpg, txt, html
                                if (!requestPath.endsWith(".jpg") && !requestPath.endsWith(".jpeg")) {
                                    if (requestPath.endsWith(".txt")) {
                                        headerKey = "text/plain";
                                    } else if (requestPath.endsWith(".html")) {
                                        headerKey = "text/html";
                                    }
                                } else {
                                    headerKey = "image/jpeg";
                                }

                                // init file read params
                                long fileStart = 0L;
                                long fileLen = StaticFileObj.length();
                                boolean isRangeReq = false;

                                // check for range header
                                if (requestHeaders.get("range") != null) {
                                    String[] rangePartsHeader = ((String) requestHeaders.get("range")).split("=");
                                    String[] rangeBound = rangePartsHeader.length > 1 ? rangePartsHeader[1].split("-") : null;

                                    // parse range bounds if valid
                                    if (rangePartsHeader[0].equals("bytes") && rangeBound != null && rangeBound.length == 2) {
                                        try {
                                            fileStart = Long.valueOf(rangeBound[0]);
                                            long fileEnd = Long.valueOf(rangeBound[1]);
                                            fileLen = fileEnd - fileStart + 1L;
                                            isRangeReq = true;


                                        } catch (Exception rangeParseException) {
                                            // handle parsing exc
                                        }
                                    }
                                }

                                // write rsp headers for file
                                responseWriter.write("HTTP/1.1 " + (isRangeReq ? "206 Range" : "200 OK") + "\r\nContent-Length: " + fileLen + "\r\nServer: AndreasServer/1.0\r\nContent-Type: " + headerKey + "\r\n");

                                // write range header if applicable
                                if (isRangeReq) {
                                    responseWriter.write("Range: bytes=" + fileStart + "-" + (fileStart + fileLen - 1L) + "/" + StaticFileObj.length() + "\r\n");
                                }

                                // write final line break and flush headers
                                responseWriter.write("\r\n");
                                responseWriter.flush();

                                // handle file read and write for GET method
                                if (httpMethod.equals("GET")) {
                                    FileInputStream fileInputStream = new FileInputStream(StaticFileObj);
                                    fileInputStream.skip(fileStart);
                                    byte[] fileBuffer = new byte[1024];

                                    // read and write file in chunks
                                    while (true) {
                                        int bytesRead = fileInputStream.read(fileBuffer);
                                        if (bytesRead <= 0) {
                                            fileInputStream.close();
                                            break;
                                        }

                                        // calc num bytes to write for curr chunk
                                        int bytesToWrite = (int) ((long) bytesRead > fileLen ? fileLen : (long) bytesRead);
                                        clientSocket.getOutputStream().write(fileBuffer, 0, bytesToWrite);
                                        fileLen -= (long) bytesToWrite;
                                    }
                                }

                                // mark rsp as sent
                                responseSent = true;
                            }
                        } else {
                            // file exists but can't be read
                            log.info("static file " + StaticFileObj.getAbsolutePath() + " exists, but cannot be read");
                            server.sendError(responseWriter, 403, "forbidden");
                            responseSent = true;
                        }


                    } else {
                        // no suitable route or static file
                        log.info("no suitable route found - and no static file " + StaticFileObj.getAbsolutePath() + " exists");
                    }
                }

                // send 404 error if no rsp sent
                if (!responseSent) {
                    server.sendError(responseWriter, 404, "not found");
                }
            }

        } catch (Exception e) {
            // handle specific exceptions
            if (e instanceof SocketTimeoutException) {
                log.warn("socket timeout for client: " + clientSocket.getRemoteSocketAddress(), e);
            } else if (e instanceof SocketException) {
                log.warn("socket exception for client: " + clientSocket.getRemoteSocketAddress(), e);
            } else {
                log.error("error handling client request for: " + clientSocket.getRemoteSocketAddress(), e);
            }
        } finally {
            try {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                    log.info("client socket closed successfully.");
                }
            } catch (IOException closeException) {
                log.error("error closing client socket", closeException);
            }
        }
    }
}
