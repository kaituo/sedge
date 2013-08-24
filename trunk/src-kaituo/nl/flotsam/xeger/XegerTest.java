/**
 * Copyright 2009 Wilfred Springer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.flotsam.xeger;

import org.junit.Test;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;

import static org.junit.Assert.assertTrue;

public class XegerTest {

    @Test
    public void shouldGenerateTextCorrectly() {
        String regex = "[ab]{4,6}c";
        Xeger generator = new Xeger(regex);
        for (int i = 0; i < 100; i++) {
            String text = generator.generate();
            assertTrue(text.matches(regex));
        }
        regex = ".*apache.*";
        generator = new Xeger(regex);
        for (int i = 0; i < 100; i++) {
            String text = generator.generate(); //"aapachee";
//            System.out.println(text);
            assertTrue(text.matches(regex));
        }
    }
    
    @Test
    public void automatonTest() {
    	RegExp r = new RegExp("ab(c|d)*");
    	Automaton a = r.toAutomaton();
    	String s = "abcccdc";
//    	System.out.println("Match: " + a.run(s)); // prints: true
    	
    	r = new RegExp(".*apache.*");
    	a = r.toAutomaton();
    	s = "apachec";
//    	System.out.println("Match: " + a.run(s)); // prints: true
    }
    
    @Test
    public void shouldGenerateTextCorrectly2() {
        String regex = ".*a";
        Xeger generator = new Xeger(regex);
        for (int i = 0; i < 100; i++) {
            String text = generator.generate();
            assertTrue(text.matches(regex));
        }
    }

    @Test
    public void shouldGenerateTextCorrectly3() {
        String regex1 = ".*a";
        String regex2 = "ba";
        Xeger generator = new Xeger(regex1, regex2);
        for (int i = 0; i < 100; i++) {
            String text = generator.generate();
            assertTrue(text.matches(regex1));
            assertTrue(text.matches(regex2));
        }
    }
    
    @Test
    public void shouldGenerateTextCorrectly4() {
        String regex1 = ".*apache.*";
        String regex2 = ".*commons.*";
        Xeger generator = new Xeger(regex1, regex2);
        for (int i = 0; i < 1; i++) {
            String text = generator.generate();
            System.out.println(text);
            assertTrue(text.matches(regex1));
            assertTrue(text.matches(regex2));
        }
    }

}