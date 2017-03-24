package com.ddp.jarmanager;

import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

import javax.tools.*;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class InlineCompiler {

    public static void compile(JclObjectFactory jclFactory, JarClassLoader jcl, String pkg, String className, List<File> helloWorldJava) {

        for(File file : helloWorldJava){
            System.out.println("filename=" + file.getAbsolutePath());
        }

        try{
                /** Compilation Requirements *********************************************************************************************/
            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);

                List<String> optionList = new ArrayList<String>();
                //optionList.add("-classpath");
                //optionList.add(System.getProperty("java.class.path") + ";dist/InlineCompiler.jar");

                Iterable<? extends JavaFileObject> compilationUnit
                        = fileManager.getJavaFileObjectsFromFiles(helloWorldJava);
                JavaCompiler.CompilationTask task = compiler.getTask(
                        null,
                        fileManager,
                        diagnostics,
                        optionList,
                        null,
                        compilationUnit);

                /********************************************************************************************* Compilation Requirements **/
                if (task.call()) {
                    /** Load and execute *************************************************************************************************/
                    System.out.println("Yipe");
                    // Create a new custom class loader, pointing to the directory that contains the compiled

                    /************************************************************************************************* Load and execute **/
                } else {
                    for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
                        System.out.format("Error on line %d in %s%n",
                                diagnostic.getLineNumber(),
                                diagnostic.getSource().toUri());
                    }
                }
                fileManager.close();

            } catch (Exception exp) {
                exp.printStackTrace();
            }
    }
}