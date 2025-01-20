package cis5550.crawling;

import java.net.*;
import java.io.*;

public class FlameSubmit {

    //holds rsp code for http req
    static int responseCode;

    //holds err response if request fails
    static String errorResponse;

    /**
     * submits a jar file to  server with a main class and arguments
     *
     * @param server server address
     * @param jarFileName jar file name to upload
     * @param className  main class name to exec
     * @param arg  args for  job
     * @return  rsp from  server if successfull else failed triggers null
     * @throws Exception issue with submit
     */
    public static String submit(String server, String jarFileName, String className, String arg[]) throws Exception {
        //assume success
        responseCode = 200;

        //reset err rsp
        errorResponse = null;

        //init creating  submit url
        String u = "http://" + server + "/submit" + "?class=" + className;
        for (int i = 0; i < arg.length; i++) {

            //push encoded args
            u = u + "&arg" + (i + 1) + "=" + URLEncoder.encode(arg[i], "UTF-8");
        }

        //read jar file into byte arr
        File f = new File(jarFileName);
        byte jarFile[] = new byte[(int) f.length()];
        FileInputStream fis = new FileInputStream(jarFileName);

        //load jar file contents and init file stream close
        fis.read(jarFile);
        fis.close();

        //open http connection to  server
        HttpURLConnection con = (HttpURLConnection) (new URI(u).toURL()).openConnection();

        //set post method
        con.setRequestMethod("POST");

        //enable connection output
        con.setDoOutput(true);

        //set content len
        con.setFixedLengthStreamingMode(jarFile.length);

        //set content type
        con.setRequestProperty("Content-Type", "application/jar-archive");

        //init connection
        con.connect();

        //send jar file data
        OutputStream out = con.getOutputStream();
        out.write(jarFile);

        try {

            //read server response
            BufferedReader r = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String result = "";
            while (true) {

                //read line by line
                String s = r.readLine();

                //reached end of rsp
                if (s == null)
                    break;

                //append response
                result = result + (result.equals("") ? "" : "\n") + s;
            }

            //return full response
            return result;
        } catch (IOException ioe) {
            //handle err msg
            responseCode = con.getResponseCode();
            BufferedReader r = new BufferedReader(new InputStreamReader(con.getErrorStream()));
            errorResponse = "";
            while (true) {

                //read err stream lines
                String s = r.readLine();

                //reached EO stream
                if (s == null)
                    break;

                //error details
                errorResponse = errorResponse + (errorResponse.equals("") ? "" : "\n") + s;
            }

            //trigger failure notif
            return null;
        }
    }

    /**
     * gets http response code from  last submission
     *
     * @return response code
     */
    public static int getResponseCode() {
        return responseCode;
    }

    /**
     * gets err response from last submission if failed
     *
     * @return err response
     */
    public static String getErrorResponse() {
        return errorResponse;
    }

    /**
     * main method to submit job by CLI
     *
     * @param args CLI args (server, jarFile, className, optional args)
     * @throws Exception if re is an issue with submission
     */
    public static void main(String args[]) throws Exception {

        //check if valid num args
        if (args.length < 3) {
            System.err.println("Syntax: FlameSubmit <server> <jarFile> <className> [args...]");
            System.exit(1);
        }

        //parse other arguments beyond first three
        String[] arg = new String[args.length - 3];
        for (int i = 3; i < args.length; i++)
            arg[i - 3] = args[i];

        try {


            //submit job and ger rsp from server
            String response = submit(args[0], args[1], args[2], arg);
            if (response != null) {
                //print rsp
                System.out.println(response);
            } else {

                //print err details
                System.err.println("*** JOB FAILED ***\n");
                System.err.println(getErrorResponse());
            }
        } catch (Exception e) {

            //broad err handler
            e.printStackTrace();
        }
    }
}