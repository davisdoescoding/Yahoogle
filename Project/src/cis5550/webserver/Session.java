package cis5550.webserver;

public interface Session {

  // returns session id (value of sessionid cookie)
  String id();

  // returns time session created, last accessed (same as system.currenttimemillis)
  long creationTime();
  long lastAccessedTime();

  // sets max active time in secs for session
  void maxActiveInterval(int seconds);

  // invalidates session (does not delete client cookie)
  void invalidate();

  // gets value for key, sets key to value
  Object attribute(String name);
  void attribute(String name, Object value);
};
