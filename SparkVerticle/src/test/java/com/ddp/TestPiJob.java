package com.ddp;


import java.io.File;
import java.net.URI;


import com.cloudera.livy.*;
import com.cloudera.livy.LivyClient;


/**
  * Created by cloudera on 3/23/17.
  */
public class TestPiJob {

        public static void main(String[] args) throws Exception {
            if (args.length != 2) {
                System.err.println("Usage: PiJob <livy url> <slices>");
                System.exit(-1);
            }

            LivyClient client = null;

            try {

                client = new LivyClientBuilder()
                        .setURI(new URI(args[0]))
                        .build();

                System.out.println("Uploading livy-example jar to the SparkContext...");
                for (String s : System.getProperty("java.class.path").split(File.pathSeparator)) {
                    if (new File(s).getName().startsWith("livy-examples")) {
                        client.uploadJar(new File(s)).get();
                        break;
                    }
                }

                final int slices = Integer.parseInt(args[1]);
                double pi = client.submit(new PiJob(slices)).get();

                System.out.println("Pi is roughly " + pi);
            }
            catch (Exception e){
                e.printStackTrace();
            }
            finally {
                client.stop(true);
            }
        }


}
