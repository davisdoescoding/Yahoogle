package cis5550.webserver;

import cis5550.tools.Logger;

// functional interface for defining routes in webserver
@FunctionalInterface
public interface Route {

    Logger log = Logger.getLogger(Route.class);

    // method to handle a req and generate response
    Object handle(Request request, Response response) throws Exception;
        }
