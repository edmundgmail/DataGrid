package com.ddp.util;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by cloudera on 3/21/17.
 */
public class ClassUtils {
    public static Class findClass(String name) {
        return findClass(name, false);
    }

    public static Class findClass(String name, boolean silence) {
        try {
            return com.ddp.utils.Utils.class.forName(name);
        } catch (ClassNotFoundException e) {
            if (!silence) {
                // LOG.error(e.getMessage(), e);
            }
            return null;
        }
    }

    public static Object invokeMethod(Object o, String name) {
        return invokeMethod(o, name, new Class[]{}, new Object[]{});
    }

    public static Object invokeMethod(Object o, String name, Class[] argTypes, Object[] params) {
        try {
            return o.getClass().getMethod(name, argTypes).invoke(o, params);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            // Logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return null;
    }

    public static Object invokeStaticMethod(Class c, String name, Class[] argTypes, Object[] params) {
        try {
            return c.getMethod(name, argTypes).invoke(null, params);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            // logger.error(e.getMessage(), e);
        }
        return null;
    }

    public static Object invokeStaticMethod(Class c, String name) {
        return invokeStaticMethod(c, name, new Class[]{}, new Object[]{});
    }

}
