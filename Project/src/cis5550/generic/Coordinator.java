package cis5550.generic;

import cis5550.webserver.Server;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Vector;

public class Coordinator {
	// map of worker IDs to their info
	static HashMap<String, WorkerEntry> workerEntries;

	// constructor (not much going on here)
	public Coordinator() {
	}

	// get list of active workers' addresses
	public static Vector<String> getWorkers() {
		// init empty list to store worker addresses
		Vector<String> workerList = new Vector<>();

		// iterate over worker IDs
		Iterator<String> workerKeys = workerEntries.keySet().iterator();
		while (workerKeys.hasNext()) {
			String workerID = workerKeys.next();
			// get worker address and port
			String workerAddress = workerEntries.get(workerID).ipAddress;
			workerList.add(workerAddress + ":" + workerEntries.get(workerID).portNumber);
		}

		// return the complete list
		return workerList;
	}

	// remove workers that haven't checked in recently
	static void cleanWorkerList() {
		// set of workers considered "stale" (dead)
		HashSet<String> staleWorkers = new HashSet<>();

		// iterate over current workers
		Iterator<String> workerKeys = workerEntries.keySet().iterator();
		while (workerKeys.hasNext()) {
			String workerID = workerKeys.next();
			// calc time since last check-in
			long timeElapsed = (System.currentTimeMillis() - workerEntries.get(workerID).lastCheckInTime.getTime()) / 1000L;

			// add to stale list if no check-in in >15 sec
			if (timeElapsed > 15L) {
				staleWorkers.add(workerID);
			}
		}

		// remove stale workers from the main list
		for (String workerID : staleWorkers) {
			System.out.println("Removing dead worker: " + workerID);
			workerEntries.remove(workerID);
		}
	}

	// generate an HTML table of workers for client display
	public static String clientTable() {
		// init html response string
		String workerTableHTML = "";

		// clean stale workers
		cleanWorkerList();

		// check if any active workers exist
		if (workerEntries.size() > 0) {
			// start building the HTML table
			workerTableHTML += "    <table border=\"1\">\n      <tr><td><b>ID</b></td><td><b>Address</b></td><td><b>Last checkin</b></td></tr>\n";

			// sort worker IDs for consistency
			TreeSet<String> sortedWorkerIDs = new TreeSet<>();
			sortedWorkerIDs.addAll(workerEntries.keySet());

			// iterate over sorted workers and add rows to the table
			for (String workerID : sortedWorkerIDs) {
				String workerAddress = workerEntries.get(workerID).ipAddress + ":" + workerEntries.get(workerID).portNumber;
				long secondsSinceCheckIn = (System.currentTimeMillis() - workerEntries.get(workerID).lastCheckInTime.getTime()) / 1000L;

				// create table row with worker info
				workerTableHTML += "      <tr><td><a href=\"http://" + workerAddress + "/\">" + workerID + "</a></td><td>" + workerAddress + "</td>";
				workerTableHTML += "<td>" + secondsSinceCheckIn + " seconds ago</td></tr>\n";
			}

			// close table
			workerTableHTML += "    </table>\n";
		} else {
			// no workers found
			workerTableHTML += "No active workers.<p>";
		}

		// return the HTML table string
		return workerTableHTML;
	}

	// register HTTP routes for coordinator
	public static void registerRoutes() {
		// init empty worker map
		workerEntries = new HashMap<>();

		// route for worker ping
		Server.get("/ping", (request, response) -> {
			// extract worker ID and port from params
			String workerID = request.queryParams("id");
			String workerPort = request.queryParams("port");

			if (workerID != null && workerPort != null) {
				// add worker entry if it doesn't exist
				workerEntries.putIfAbsent(workerID, new WorkerEntry());

				// update worker info
				WorkerEntry workerData = workerEntries.get(workerID);
				workerData.lastCheckInTime = new Date(); // update check-in time
				workerData.ipAddress = request.ip();     // update IP
				workerData.portNumber = Integer.parseInt(workerPort); // update port

				// send OK response
				response.type("text/plain");
				return "OK";
			} else {
				// send error if params are missing
				response.status(400, "Bad request");
				return "One of the two required parameters (id, port) is missing";
			}
		});

		// route for listing workers
		Server.get("/workers", (request, response) -> {
			// clean stale workers
			cleanWorkerList();

			// set response type to plain text
			response.type("text/plain");

			// sort worker IDs
			TreeSet<String> sortedWorkerIDs = new TreeSet<>();
			sortedWorkerIDs.addAll(workerEntries.keySet());

			// build response with worker info
			String workerInfo = sortedWorkerIDs.size() + "\n";

			for (String workerID : sortedWorkerIDs) {
				workerInfo += workerID + "," + workerEntries.get(workerID).ipAddress + ":" + workerEntries.get(workerID).portNumber + "\n";
			}

			// return worker list info
			return workerInfo;
		});
	}

	// class to hold worker info
	static class WorkerEntry {
		String ipAddress;        // worker IP
		int portNumber;          // worker port
		Date lastCheckInTime;    // last time worker checked in

		// default constructor
		WorkerEntry() {
		}
	}
}
