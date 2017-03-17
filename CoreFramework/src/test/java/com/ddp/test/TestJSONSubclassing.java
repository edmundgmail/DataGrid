package com.ddp.test;

/**
 * Created by cloudera on 2/26/17.
 */
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class TestJSONSubclassing {

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        ClientObject c = new ClientObject();

        MyItem i1 = new MySubItemA(1, "Value1", "Some stuff", "Another property value");
        MyItem i2 = new MySubItemB(2, "Value2", 1000, "B property");
        MyItem i3 = new MySubItemC(3, "Value3", 2000, -1);
        MyItem i4 = new MySubItemA(4, "Value4", "Bla Bla Bla", "item A property");

        c.getL().add(i1);
        c.getL().add(i2);
        c.getL().add(i3);
        c.getL().add(i4);

        String s = null;
        try {
            s = mapper.writeValueAsString(c);
        }
        catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println(s);

        ClientObject c2 = null;
        try {
            c2 = mapper.readValue(s, ClientObject.class);
        }
        catch (JsonParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if (c2 != null) {
            System.out.println("----- Items List -----");

            for (MyItem mi : c2.getL()) {
                System.out.println("Type = " + mi.getClass() +  ", id = "+ mi.getId() + ", name = " + mi.getName());
            }
        }
    }
}