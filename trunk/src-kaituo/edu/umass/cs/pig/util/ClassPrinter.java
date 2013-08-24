package edu.umass.cs.pig.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ClassPrinter {
	public void print(Object obj) {
		for (Field f : obj.getClass().getDeclaredFields()) {
            f.setAccessible(true);
            System.out.println(f.getName());
        }
		
		for (Method m : obj.getClass().getDeclaredMethods()) {
            m.setAccessible(true);
            System.out.println(m.getName());
        }
		
		for (Constructor<?> c : obj.getClass().getDeclaredConstructors()) {
            c.setAccessible(true);
            System.out.println(c.getName());
        }
	}

}
