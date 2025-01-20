package cis5550.kvs;

import cis5550.webserver.*;

public class Coordinator extends cis5550.generic.Coordinator {

	public static void main(String[] args) throws Exception {


		// check if correct num of args provided
		if (args.length != 1) {
			// log error if args are missing or extra
			System.out.println("incorrect number of args provided. Syntax: java cis5550.kvs.Coordinator portNumber");
			// exit program with error
			System.exit(1);
		}

		// parse port num from args and set server port
		int portNumber = Integer.parseInt(args[0]);
		System.out.println("Starting KVS Coordinator on port: " + portNumber);
		Server.port(portNumber);

		// register routes for handling reqs
		System.out.println("Registering routes...");
		registerRoutes();

		// default route to show client table in HTML
		Server.get("/", (request, response) -> {
			System.out.println("Received request for client table...");
			return "<html><head><title>KVS Coordinator</title></head><body><h3>KVS Coordinator</h3>\n" + clientTable() + "</body></html>";
		});

		// route to return version info
		Server.get("/version", (request, response) -> {
			System.out.println("Returning version info...");
			return "v1.3 Oct 28 2022";
		});

		System.out.println("KVS Coordinator is up and running.");
	}
}
