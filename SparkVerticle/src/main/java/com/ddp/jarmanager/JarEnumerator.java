package com.ddp.jarmanager;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by cloudera on 3/24/17.
 */
public class JarEnumerator {
    public static List<String> getClazzNames(String jarFile) {
        try{
            FileInputStream stream = new FileInputStream(jarFile);
            return getClazzNames(stream);
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public static List<String> getClazzNames(InputStream stream) {

        List<String> classNames = new ArrayList<String>();

        try{
            ZipInputStream zip = new ZipInputStream(stream);
            for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                if (!entry.isDirectory() && entry.getName().endsWith(".class")) {
                    // This ZipEntry represents a class. Now, what class does it represent?
                    String className = entry.getName().replace('/', '.'); // including ".class"
                    classNames.add(className.substring(0, className.length() - ".class".length()));
                }
            }

        }
        catch (Exception e){
            e.printStackTrace();
        }

        return classNames;
    }
}
