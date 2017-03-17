package com.ddp.test;

/**
 * Created by cloudera on 2/26/17.
 */
public class MySubItemC extends MyItem {

    private int itemCProperty1;
    private int itemCProperty2;

    public int getItemCProperty1() {
        return this.itemCProperty1;
    }

    public void setItemCProperty1(int itemCProperty1) {
        this.itemCProperty1 = itemCProperty1;
    }

    public int getItemCProperty2() {
        return this.itemCProperty2;
    }

    public void setItemCProperty2(int itemCProperty2) {
        this.itemCProperty2 = itemCProperty2;
    }

    public MySubItemC(int id, String name, int p1, int p2) {
        super(id, name);
        this.itemCProperty1 = p1;
        this.itemCProperty2 = p2;
    }
}