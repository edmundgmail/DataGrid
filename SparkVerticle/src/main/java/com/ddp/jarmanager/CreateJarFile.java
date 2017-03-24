package com.ddp.jarmanager;

import org.apache.commons.io.filefilter.HiddenFileFilter;

import java.io.*;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;


public class CreateJarFile {

    public static String mkJar(File classes, String mainClassName) throws IOException
    {
        String jarName = mainClassName + System.currentTimeMillis() + "_" + (new java.util.Random()).nextInt(10000) + ".jar";
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, ".");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, mainClassName); //saw somewhere it was just getName. not sure if it's right tho. it doesn't work :(
        JarOutputStream target = new JarOutputStream(new FileOutputStream(jarName), manifest);

        //add(classes, target); //not sure if this is right tho.
        for(File f : classes.listFiles(/*new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if(pathname.isFile() && pathname.getName().endsWith(".class")) return true;
                return false;
            }
        }*/)){
            add(f, target, classes.getName());
        }

        target.close();
        return jarName;
    }

    private static void add(File source, JarOutputStream target, String leadingName) throws IOException
    {
        BufferedInputStream in = null;
        try
        {
            if (source.isDirectory())
            {
                String name = source.getPath().replace("\\", "/");
                if (!name.isEmpty())
                {
                    if (!name.endsWith("/"))
                        name += "/";
                    JarEntry entry = new JarEntry(name.replaceFirst(leadingName+"/", ""));
                    entry.setTime(source.lastModified());
                    target.putNextEntry(entry);
                    target.closeEntry();
                }
                for (File nestedFile: source.listFiles((FileFilter) HiddenFileFilter.VISIBLE)) {
                    add(nestedFile, target, leadingName);
                }
                return;
            }

            JarEntry entry = new JarEntry(source.getPath().replace("\\", "/").replaceFirst(leadingName+"/", ""));
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            in = new BufferedInputStream(new FileInputStream(source));

            byte[] buffer = new byte[1024];
            while (true)
            {
                int count = in.read(buffer);
                if (count == -1)
                    break;
                target.write(buffer, 0, count);
            }
            target.closeEntry();
        }
        finally
        {
            if (in != null)
                in.close();
        }
    }
}