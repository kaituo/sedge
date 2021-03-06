package edu.umass.cs.pig;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Help text. Idea is to access JavaDoc kind of text comment at runtime.
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Help {
	String value();
}

