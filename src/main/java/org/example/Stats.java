package org.example;

import java.io.Serializable;

public class Stats implements Serializable {
   String routrplusnumberkey;
    String  windowStart;
   String windowEnd;
    Long value;

    public Stats(){

    }
    public Stats(String ws, String we, Long val, String trk){
        this.value= val;
        this.windowEnd = we;
        this.windowStart = ws;
        this.routrplusnumberkey = trk;
    }
}
