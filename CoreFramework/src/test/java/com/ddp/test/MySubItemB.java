package com.ddp.test;

/**
 * Created by cloudera on 2/26/17.
 */
public class MySubItemB extends MyItem {

    private int itemBProperty1;
    private String itemBProperty2;

    public int getItemBProperty1() {
        return this.itemBProperty1;
    }

    public void setItemBProperty1(int itemBProperty1) {
        this.itemBProperty1 = itemBProperty1;
    }

    public String getItemBProperty2() {
        return this.itemBProperty2;
    }

    public void setItemBProperty2(String itemBProperty2) {
        this.itemBProperty2 = itemBProperty2;
    }

    public MySubItemB(int id, String name, int p1, String p2) {
        super(id, name);
        this.itemBProperty1 = p1;
        this.itemBProperty2 = p2;
    }
}