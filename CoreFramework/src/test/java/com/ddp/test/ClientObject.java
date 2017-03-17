package com.ddp.test;

/**
 * Created by cloudera on 2/26/17.
 */
import java.util.ArrayList;
import java.util.List;

public class ClientObject {

    private List<MyItem> l;

    public ClientObject(List<MyItem> pl) {
        this.l = pl;
    }

    public ClientObject() {
        this.l = new ArrayList<MyItem>();
    }

    public List<MyItem> getL() {
        return this.l;
    }

    public void setL(List<MyItem> l) {
        this.l = l;
    }
}