package cis5550.generic;

import cis5550.tools.HTTP;
import java.io.IOException;

public class Worker {

	// constructor 4 Worker class (nothing special here)
	public Worker() {
	}

	// starts thread that pings coordinator periodically
	public static void startPingThread(final String coordinatorAddress, final String workerID, final int port) {

		// create and start new ping thread
		(new Thread("Ping thread") {
			public void run() {

				// infinite loop to keep pinging
				while (true) {
					try {
						// log ping attempt
//						System.out.println("pinging coordinator: " + coordinatorAddress + " from worker: " + workerID);

						// make GET request to coordinator's ping endpoint
						HTTP.Response response = HTTP.doRequest(
								"GET",
								"http://" + coordinatorAddress + "/ping?id=" + workerID + "&port=" + port,
								(byte[]) null
						);

						// log successful ping
//						System.out.println("ping successful. coordinator response: " + response.statusCode());
					} catch (IOException e) {
						// log error if ping fails
						System.err.println("unable to ping coordinator at " + coordinatorAddress);
					}

					try {
						// sleep for 10 sec before next ping
//						System.out.println("sleeping 10 sec before next ping...");
						sleep(10000L);
					} catch (InterruptedException e) {
						System.err.println("ping thread interrupted.");
					}
				}
			}
		}).start(); // start thread
	}
}
