package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

class SessionImpl implements Session {
    String sID;
    long sCreationTime;
    long sLastAccessedTime;
    int sMaxActiveInterval;
    Map<String, Object> data;

    SessionImpl() {
        Random var1 = new Random();
        this.sID = "";

        for(int var2 = 0; var2 < 20; ++var2) {
            String var10001 = this.sID;
            this.sID = var10001 + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789[]".charAt(var1.nextInt(64));
        }

        this.sCreationTime = System.currentTimeMillis();
        this.sLastAccessedTime = System.currentTimeMillis();
        this.sMaxActiveInterval = 300;
        this.data = new HashMap();
    }

    public String id() {
        return this.sID;
    }

    public long creationTime() {
        return this.sCreationTime;
    }

    public long lastAccessedTime() {
        return this.sLastAccessedTime;
    }

    public int maxActiveInterval() {
        return this.sMaxActiveInterval;
    }

    void updateLastAccessed() {
        this.sLastAccessedTime = System.currentTimeMillis();
    }

    public void maxActiveInterval(int var1) {
        this.sMaxActiveInterval = var1;
    }

    public void invalidate() {
        this.sLastAccessedTime = 0L;
    }

    public Object attribute(String var1) {
        return this.data.get(var1);
    }

    public void attribute(String var1, Object var2) {
        this.data.put(var1, var2);
    }
}
