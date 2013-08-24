package nl.flotsam.xeger;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicOperations;
import dk.brics.automaton.State;
import dk.brics.automaton.Transition;
import dk.brics.automaton.RegExp;

import java.util.Random;
import java.util.List;

/**
 * An object that will generate text from a regular expression. In a
way, it's the opposite of a regular expression
 * matcher: an instance of this class will produce text that is
guaranteed to match the regular expression passed in.
 */
public class Xeger {

   private final Automaton automaton;
   private final Random random;

   /**
    * Constructs a new instance, accepting the regular expression and
the randomizer.
    *
    * @param regex  The regular expression. (Not <code>null</code>.)
    * @param random The object that will randomize the way the String
is generated. (Not <code>null</code>.)
    * @throws IllegalArgumentException If the regular expression is invalid.
    */
   public Xeger(String regex, Random random) {
       assert regex != null;
       assert random != null;
       if(regex.contains(".*")) {
           regex = regex.replace(".*", "[a-zA-Z0-9]{0,9}");
       }

       if(regex.contains(".")) {
           regex = regex.replace(".", "[a-zA-Z0-9]");
       }
       this.automaton = new RegExp(regex).toAutomaton();
       this.random = random;
   }

   public Xeger(String regex1, String regex2, Random random) {
       assert regex1 != null;
       assert regex2 != null;
       assert random != null;
       if(regex1.contains(".*")) {
           regex1 = regex1.replace(".*", "[a-zA-Z0-9]{0,9}");
       }

       if(regex1.contains(".")) {
           regex1 = regex1.replace(".", "[a-zA-Z0-9]");
       }
       if(regex2.contains(".*")) {
           regex2 = regex2.replace(".*", "[a-zA-Z0-9]{0,9}");
       }

       if(regex2.contains(".")) {
           regex2 = regex2.replace(".", "[a-zA-Z0-9]");
       }
       Automaton automaton1 = new RegExp(regex1).toAutomaton();
       Automaton automaton2 = new RegExp(regex2).toAutomaton();
       this.automaton = BasicOperations.intersection(automaton1, automaton2);
       this.random = random;
   }

   /**
    * As {@link nl.flotsam.xeger.Xeger#Xeger(String,
java.util.Random)}, creating a {@link java.util.Random} instance
    * implicityly.
    */
   public Xeger(String regex) {
       this(regex, new Random());
   }

   public Xeger(String regex1, String regex2) {
       this(regex1, regex2, new Random());
   }

   /**
    * Generates a random String that is guaranteed to match the
regular expression passed to the constructor.
    */
   public String generate() {
       StringBuilder builder = new StringBuilder();
       generate(builder, automaton.getInitialState());
       return builder.toString();
   }

   private void generate(StringBuilder builder, State state) {
       List<Transition> transitions = state.getSortedTransitions(true);
       if (transitions.size() == 0) {
           assert state.isAccept();
           return;
       }
       int nroptions = state.isAccept() ? transitions.size() :
transitions.size() - 1;
       int option = XegerUtils.getRandomInt(0, nroptions, random);
       if (state.isAccept() && option == 0) {          // 0 is considered stop
           return;
       }
       // Moving on to next transition
       Transition transition = transitions.get(option -
(state.isAccept() ? 1 : 0));
       appendChoice(builder, transition);
       generate(builder, transition.getDest());
   }

   private void appendChoice(StringBuilder builder, Transition transition) {
       char c = (char) XegerUtils.getRandomInt(transition.getMin(),
transition.getMax(), random);
       builder.append(c);
   }

}