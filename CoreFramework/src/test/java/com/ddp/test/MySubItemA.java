package com.ddp.test;

/**
 * Created by cloudera on 2/26/17.
 */
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MySubItemA extends MyItem {

    private String itemAProperty1;
    private String itemAProperty2;

    public String getItemAProperty1() {
        return this.itemAProperty1;
    }

    public void setItemAProperty1(String itemAProperty1) {
        this.itemAProperty1 = itemAProperty1;
    }

    public String getItemAProperty2() {
        return this.itemAProperty2;
    }

    public void setItemAProperty2(String itemAProperty2) {
        this.itemAProperty2 = itemAProperty2;
    }

    @JsonCreator
    public MySubItemA(@JsonProperty("id")int id, @JsonProperty("name")String name, @JsonProperty("itemAProperty1")String p1, @JsonProperty("itemAProperty2")String p2) {
        super(id, name);
        this.itemAProperty1 = p1;
        this.itemAProperty2 = p2;
    }

}