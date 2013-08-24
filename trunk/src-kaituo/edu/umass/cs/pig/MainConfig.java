package edu.umass.cs.pig;

import static edu.umass.cs.pig.util.Log.logln;
import edu.umass.cs.pig.util.Log;
import gnu.trove.THashSet;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Enumeration;
import java.util.Properties;


/**
 * Constant values
 *
 * <p>Notice our convention of passing Z3PARAM_ parameters to Z3.</p>
 *
 * TODO: Can we merge this with the instrumentation's Config class?
 *
 * @author csallner@uta.edu (Christoph Csallner)
 */
public class MainConfig implements Serializable {

  /**
	 * 
	 */
	private static final long serialVersionUID = -2634148771317717512L;
  private static MainConfig singleton = new MainConfig();
  public static boolean debug = true;

  /**
   * Singleton
   */
  public static MainConfig get() {
    return singleton;
  }

  /**
   * If no config set yet, then set a new MainConfig.
   */
  public static MainConfig setInstance() {
    if (singleton==null)
      singleton = new MainConfig();
    return get();
  }

  
  
  /**
   * If no config set yet, then set conf.
   * This method should only be called if conf is an
   * instance of a sub-class of MainConfig.
   */
  public static MainConfig setInstance(MainConfig conf) {
    if (singleton==null)
      singleton = conf;
    return get();
  }



  /**
   * Constructor.
   *
   * <p><b>Should only be called from subclass constructor</b>
   * (or {@link #setInstance()}).
   */
  protected MainConfig() {
    // empty
  }

  /**
   * Name of the native code library that wraps Z3
   * Kaituo: added support for customizing z3Wrap, z3WrapDll, Z3_WRAP_PATH
   * 			according to OSs; z3Wrap and z3WrapDll are no longer final
   * 			any more
   */
  public  String z3Wrap; //$NON-NLS-1$
  public  String z3WrapDll; //$NON-NLS-1$
 
  @Help("Full directory of Z3Wrap.dll. Set this option if Dsc cannot locate Z3Wrap.dll")
  public String Z3_WRAP_PATH = "/java/dsc/"; //$NON-NLS-1$
  
  @Help("Basic Dsc explores method BASIC_DSC_CLASS_CP_METHOD " +
  		"in this class from the class path. " +
  		"Ignored by RoopsDsc.")
  public String BASIC_DSC_CLASS_CP = "please.Cover"; //$NON-NLS-1$

  @Help("Basic Dsc explores this method in BASIC_DSC_CLASS_CP. " +
  		"Ignored by RoopsDsc.")
  public String BASIC_DSC_CLASS_CP_METHOD = "me"; //$NON-NLS-1$
  
  @Help("RoopsDsc explores these Roops classes from the class path " +
  		"(independently of ROOPS_DSC_DIRS). " +
  		"Ignored by basic Dsc.")
  public LinkedStringList ROOPS_DSC_CLASSES_CP = new LinkedStringList();

  @Help("RoopsDsc explores all Roops classes in ROOPS_DSC_DIRS that " +
  		"match a pattern in the ROOPS_DSC_DIRS_PACKAGES property " +
  		"(independently of ROOPS_DSC_CLASSES). " +
  		"Ignored by basic Dsc.")
  public LinkedStringList ROOPS_DSC_DIRS = new LinkedStringList();

  @Help("Patterns of package prefixes RoopsDsc uses for ROOPS_DSC_DIRS. " +
  		"E.g.,: 'a.' or 'a.b.' etc. " +
  		"Ignored by basic Dsc.")
  public LinkedStringList ROOPS_DSC_DIRS_PACKAGES = new LinkedStringList();  
  

  @Help("Do not instrument any of the classes that have any of these prefixes. " +
  		"IMPORTANT: InstrumentConfig maintains the same list. " +
  		"Both lists have to be updated for Dsc to work properly! " +
  		"TODO: Merge these two lists.")
  public LinkedStringList DO_NOT_INSTRUMENT_PREFIXES =
    new LinkedStringList(
      "java.", //$NON-NLS-1$
      "javax.",  //$NON-NLS-1$  // FIXME: Allow instrumentation of javax: Deal with extension class loader
      "sun.", //$NON-NLS-1$
      "com.sun.", //$NON-NLS-1$
      "$", //$NON-NLS-1$
      
      "org.eclipse.", //$NON-NLS-1$

      "gnu.trove.", //$NON-NLS-1$

      "org.junit.", //$NON-NLS-1$
      "junit.", //$NON-NLS-1$

      "spoon.", //$NON-NLS-1$

      "edu.uta.cse.dsc.", //$NON-NLS-1$
      "edu.uta.cse.dbtest.", //$NON-NLS-1$
      "edu.uta.cse.tada.", //$NON-NLS-1$
      "edu.uta.cse.z3.", //$NON-NLS-1$

      "org.objectweb.asm.", //$NON-NLS-1$

      "roops.util.",  //$NON-NLS-1$       // Reachability benchmarks

      "org.apache.derby.",  //$NON-NLS-1$ // TaDa
      "tada.sqlparser.", //$NON-NLS-1$
      "Zql.", //$NON-NLS-1$
      "org.jcp.", //$NON-NLS-1$

      "org.postgresql.", //$NON-NLS-1$

      "edu.umass.cs.",  //$NON-NLS-1$       //Kaituo

//      "icse2010.",  //$NON-NLS-1$   // Ishtiaque: ICSE 2010
      "woda2010.",   //$NON-NLS-1$   // Ishtiaque: WODA 2010
      "ecoop2010.",		 //$NON-NLS-1$// Ishtiaque: ECOOP 2010
      "com.accenture.lab.crest.vm.",  //$NON-NLS-1$//ISHTIAQUE: CREST
      "com.mysql.",  //$NON-NLS-1$     //Ishtiauqe MySQL access
      "edu.uci.",					// Ishtiaque Repair visualization (JUNG)
      "org.apache.commons.",
      "demsky."						// Ishtiaque: Demsky example of Data Structure repair
  );


  @Help("Prefixes of classes that will always be explored symbolically, " +
  		"regardless if they appear in DO_NOT_INSTRUMENT_PREFIXES. " +
  		"IMPORTANT: InstrumentConfig maintains the same list. " +
  		"Both lists have to be updated for Dsc to work properly! " +
  		"TODO: Merge these two lists.")
  public LinkedStringList DO_INSTRUMENT_PREFIXES =
    new LinkedStringList(
        "com.sun.javadoc.", //$NON-NLS-1$
        "roops.util.RoopsArray" //$NON-NLS-1$
  );  
  
  
  public LinkedStringList DO_NOT_PLACE_MOCKCLASS_IN_PACKAGE_PREFIXES =
    new LinkedStringList(
        "java.", //$NON-NLS-1$
        "javax." //$NON-NLS-1$
  );  
  
  
  public String getz3Wrap() {
	  if (OSValidator.isWindows()) {
		  z3Wrap = "Z3Wrap"; //$NON-NLS-1$
	  } else if (OSValidator.isUnix()) {
		  z3Wrap = "z3wrap";
	  } else {
		  System.err.println("Unsupported OS.");
	  }
	  return z3Wrap;
  }
  
  public String getz3WrapDll() {
	  if (OSValidator.isWindows()) {
		  z3WrapDll = z3Wrap+".dll";  //$NON-NLS-1$
	  } else if (OSValidator.isUnix()) {
		  z3WrapDll = "";
	  } else {
		  System.err.println("Unsupported OS.");
	  }
	  return z3WrapDll;
  }
  
  public String getZ3_WRAP_PATH() {
	  if (OSValidator.isWindows()) {
		  Z3_WRAP_PATH = "/java/dsc/";  //$NON-NLS-1$
	  } else if (OSValidator.isUnix()) {
		  Z3_WRAP_PATH = "";
	  } else {
		  System.err.println("Unsupported OS.");
	  }
	  return Z3_WRAP_PATH;
  }
  
  /**
   * Override the values of all non-final public fields set in userConfigValues.
   * 
   * @param userConfigValues chain of hash-tables, read only via getProperty.
   */
  protected void apply(Properties userConfigValues) 
  {
    THashSet<String> retrievedUserOptions = new THashSet<String>();
    
    /* Check each field if we should override it */
    Field[] fields = singleton.getClass().getFields();
    for (Field field: fields) 
    {
      if (Modifier.isFinal(field.getModifiers()))
        continue;   // skip final fields
      String fieldName = field.getName();
      String value = userConfigValues.getProperty(fieldName);
      if (value==null)
        continue;
      try 
      {
        retrievedUserOptions.add(fieldName);
        Class<?> fieldType = field.getType();
        if (String.class.equals(fieldType))
          field.set(singleton, value);
        else if (int.class.equals(fieldType))
          field.set(singleton, Integer.parseInt(value));
        else if (boolean.class.equals(fieldType))
          field.set(singleton, Boolean.parseBoolean(value));
        else if (LinkedStringList.class.equals(fieldType))
          field.set(singleton, new LinkedStringList(value));
        else
          logln("Warning: could not apply unsupported value of type ", fieldType.toString());
      }
      catch(IllegalAccessException iae) {
        iae.printStackTrace();
      }
    }
    
    /* Beyond our fields, does userConfigValues contain more values? 
     * Flatten chain of userConfigValues hash-tables */
    Enumeration<String> userConfigKeys = 
      (Enumeration<String>) userConfigValues.propertyNames();
    while (userConfigKeys.hasMoreElements())
    {
      String next = userConfigKeys.nextElement();
      if (! retrievedUserOptions.contains(next))
        logln("Warning: could not apply unrecognized option ", next);
    }
  }

  
  /**
   * Set all public non-final boolean fields that start with "LOG_" to value.
   */
  public void setEachLogTo(boolean value) {
    Field[] fields = singleton.getClass().getFields();
    for (Field field: fields) {
      if (Modifier.isFinal(field.getModifiers()))
        continue;   // skip final fields
      if (! field.getName().startsWith("LOG_")) //$NON-NLS-1$
        continue;
      try {
        Class<?> fieldType = field.getType();
        if (boolean.class.equals(fieldType))
          field.setBoolean(singleton, value);
      }
      catch(IllegalAccessException iae) {
        iae.printStackTrace();
      }
    }
  }


  @Help("Dsc writes Java source code into this directory, i.e., JUnit test cases.")
  public String OUT_DIR_SRC = "generated"; //$NON-NLS-1$

  @Help("Dsc writes Java bytecode into this directory, i.e., mock classes.")
  public String OUT_DIR_BIN = "generated-bin"; //$NON-NLS-1$

  
  /* JUNIT options of generated JUnit test cases */
  
  @Help("JavaDoc comment for each generated class")
  public String JUNIT_TEST_CLASS_COMMENT = " "; //$NON-NLS-1$
  
  @Help("Tab size used in the generated JUnit test files")
  public int JUNIT_TEST_TAB_SIZE = 2;  
  
  @Help("Add an @AfterClass method to each generated test class")
  public boolean JUNIT_AFTER_CLASS_METHOD = false;

  @Help("@AfterClass annotation")
  public String JUNIT_AFTER_CLASS_ANNOTATION = "org.junit.AfterClass"; //$NON-NLS-1$

  @Help("Annotate each generated test case method as a test, e.g., with @Test")
  public boolean JUNIT_TEST_CASE_ANNOTATED_TEST = true;

  @Help("Annotation of a test case, e.g., @Test")
  public String JUNIT_TEST_CASE_ANNOTATION_TEST = "org.junit.Test"; //$NON-NLS-1$
  
  @Help("Annotate each generated test case method as generated, e.g., with @Generated")
  public boolean JUNIT_TEST_CASE_ANNOTATED_GENERATED = false;  

  @Help("Mandatory parameter of the @Generated annotation")
  public String JUNIT_TEST_CASE_ANNOTATION_GENERATED_GENERATOR = "edu.uta.cse.dsc"; //$NON-NLS-1$

  @Help("Prefix of each local variable in generated JUnit test case")
  public String JUNIT_LOCAL_VARIABLE_PREFIX = "local"; //$NON-NLS-1$


  /* MOCK options */
  
  @Help("Postfix for mock class method return value variable names")
  public String MOCK_CLASS_METHOD_RETURN_VAR_POSTFIX = "#MCRV"; //$NON-NLS-1$
  
  @Help("Prefix of generated mock class names")
  public String MOCK_CLASS_NAME_PREFIX = "MockClass"; //$NON-NLS-1$
  
  @Help("Allow a mock class to extend another mock class")
  public boolean MOCK_CLASS_MAY_EXTEND_MOCK_CLASS = false;  

  @Help("Minimum number of mock classes. Only relevant in MOCK_MODE.")
  public int MOCK_NR_MOCK_CLASSES_MIN = 0;
  
  @Help("Maximum number of mock classes. Only relevant in MOCK_MODE.")
  public int MOCK_NR_MOCK_CLASSES_MAX = 2;
  
  @Help("Delta for subsequent nr of mock classes. Only relevant in MOCK_MODE.")
  public int MOCK_NR_MOCK_CLASSES_STEP = 1;
  
  @Help("After finding a solution with mock classes, try find another one using fewer mock classes.")
  public boolean MOCK_MINIMIZE_NR_MOCK_CLASSES_USED = false;
  
  @Help("Number of mock classes cannot be larger than the number of reference parameters of the explored method.")
  public boolean MOCK_NR_MOCK_CLASSES_AT_MOST_NR_REF_PARAMS = true;  
  
  @Help("Package name used for mock classes in case we cannot infer one.")
  public String MOCK_FALLBACK_PACKAGE_NAME_FOR_MOCK_CLASSES = "mock"; //$NON-NLS-1$
  
  
  /* General options */
  
  @Help("Encode hard-to-subclass classes as final")
  public boolean ENCODE_HARD_TO_SUBCLASS_AS_FINAL = true;
  
  
  final public String MODE_DSC = "DSC"; //$NON-NLS-1$
  final public String MODE_REPAIR = "REPAIR"; //$NON-NLS-1$
  final public String MODE_ROOPS = "ROOPS"; //$NON-NLS-1$
  final public String MODE_TADA = "TADA"; //$NON-NLS-1$
  final public String MODE_DYSY = "DYSY"; //$NON-NLS-1$

  @Help("Dsc mode. Also see REPAIR_MODE and MOCK_MODE")
  public String MODE = MODE_DSC;

  // FIXME: Combine MODE_REPAIR and REPAIR_MODE into single concept
  @Help("Data-structure repair mode. Also see MODE. Maintained by Ishtiaque.")
  public boolean REPAIR_MODE = false;
  
  @Help("Only monitor execution and dump each symbolic expression immediately to disk.")
  public boolean DUMPER_MODE = false;
  
  
  @Help("REQ-1 mode.")
  public boolean REQ_1_MODE = false;
  
  
  /* DBTEST options 
   * FIXME(Mahbub): Move all DB-Test options into a separate class */
  
  @Hide
  @Help("DBTest mode maintained by Mahbub.")
  public boolean DBTEST_MODE = false;

  @Hide
  @Help("DBTEST experiments: server name")
  public String DBTEST_SERVER_URL = "jdbc:postgresql://"; //$NON-NLS-1$  
  
  @Hide
  @Help("DBTEST experiments: database name")
  public String DBTEST_DB_NAME = "db"; //$NON-NLS-1$
  
  @Hide
  @Help("DBTEST experiments: table name")
  public String DBTEST_TABLE_NAME = "t"; //$NON-NLS-1$
  
  @Hide
  @Help("DBTEST experiments: user name")
  public String DBTEST_USER_NAME = "u"; //$NON-NLS-1$
  
  @Hide
  @Help("DBTEST experiments: user password")
  public String DBTEST_USER_PASSWORD = "p"; //$NON-NLS-1$  
  
  @Hide
  @Help("Value used to divide the estimated cost from EXPLAIN STATEMENT")
  public int DBTEST_EXPLAIN_FACTOR = 100;
  
  @Hide
  @Help("Value used in initial query to limit result set: (SELECT x LIMIT 10).")
  public int DBTEST_SELECT_LIMIT = 1;
  
  @Hide
  @Help("Estimated cost by explain is divided by this factor ")
  public int DBTEST_EXPLAINCOST_FACTOR=100;
 
  @Hide
  @Help("Count number of tuples in DBTEST")
  public boolean DBTEST_COUNTTUPLES = false;
  
  @Hide
  @Help("flag to write the summary contents in a file")
  public boolean DBTEST_SUMMARY_TABLES = false;
  
  @Hide
  @Help("Printing candidates and scoring info")
  public boolean DBTEST_PRINT = false;
  
  @Hide
  @Help("arbitrarily choose a leaf candidate query, and fetch only one result tuple")
  public boolean DBTEST_LEAFQUERY_ONE = false;
  
  @Hide
  @Help("arbitrarily choose a leaf candidate query, and fetch all result tuples")
  public boolean DBTEST_LEAFQUERY_ALL = false;
  
  @Hide
  @Help("choose the leaf candidate query with the smallest estimated count of results, and fetch all result tuples")
  public boolean DBTEST_SMALL_LEAFQUERY_ALL = false;
  
  @Hide
  @Help("choose the leaf candidate query with the largest estimated count of results, and fetch all result tuples")
  public boolean DBTEST_LARGE_LEAFQUERY_ALL = false;
  
  @Hide
  @Help("choose the leaf candidate query with the smallest estimated execution time, and fetch all result tuples")
  public boolean DBTEST_SMALLESTTIME_LEAFQUERY_ALL = false;
  
  @Hide
  @Help("baseline 2: always get all the tuples of query �select * from table�  (this is different from baseline)")
  public boolean DBTEST_BASELINE_ALL = false;
  
  @Hide
  @Help("baseline: Keep going through the tuples of query �select * from table� till all nodes are covered")
  public boolean DBTEST_BASELINE = false;
  

  
  /* REPAIR options */

  // ISHTIAQUE
  @Help("Simulate multiple repair by Dsc")
  public boolean REPAIR_ATTEMPTED = false;
  
  @Help("Log debug information during repair reflection")
  public boolean REPAIR_LOG_REFLECTION = true;
  
  @Help("Log constraints collected during repair")
  public boolean REPAIR_LOG_CONSTRAINTS = true;

  @Help("Repair: Number of nodes in a binary tree")
  public int REPAIR_NO_OF_NODES = 10;

  
  /* CREST / CARFAST options */
  
  @Help("CREST: Combinatorial testing with Dsc. Maintained by Ishtiaque.")
  public boolean CREST_MODE = false;

  @Help("CarFast: Maximum iteration for CarFast")
  public int CARFAST_MAX_ITERATION = 10;

  
  /* DUMP options */
  
  @Help("File to which Dsc dumps its output in dumper mode.")
  public String DUMP_FILE_NAME = "cond.txt"; //$NON-NLS-1$
  
  
  
  @Help("Mock class generation. Also see MODE. Maintained by Mainul.")
  public boolean MOCK_MODE = false;


  /* General exploration options */
  
  @Help("Guard for any bounds-checking for the following exploration bounds.")
  public boolean USE_MAX = false;

  @Help("USE_MAX ==> max #callbacks Dsc will process for one exploration " +
  		"(set of execution paths through a single user entry method)")
  public int MAX_CALLBACKS_METHOD = 100000;

  @Help("USE_MAX ==> max #callbacks Dsc will process for a single execution path through user code")
  public int MAX_CALLBACKS_PATH = 20000;

  @Help("USE_MAX ==> Maximum number of SAT calls Dsc will issue for any given " +
  "exploration (= entry method)")
  public int MAX_SAT_CALLS_METHOD = 20;
  
  // TODO: implement MAX_SAT_CALLS_PATH
//  @Help("USE_MAX ==> Maximum number of SAT calls Dsc will issue for a single " +
//  "execution path through user code")  
//  public int MAX_SAT_CALLS_PATH = 5;

  @Help("Array creations with more elements will trigger to discard " +
  		"this path and search for a cheaper one." +
  		"This is a coarse model of OutOfMemoryError triggered by JVM.")
  public int MAX_ARRAY_SIZE_CREATION = 10000;



  @Help("Rewrite select expressions")
  public boolean SIMPLIFY_SELECT_EXPRESSIONS = true;

  @Help("Do not ask SAT solver queries that are trivially UNSAT." +
  		"Our own light-weight SAT checker.")
  public boolean SUPPRESS_TRIVIAL_UNSAT_QUERIES = true;

  @Help("Maximum sum of array variable lengths to try")
  public int ARRAY_LENGTH_MAX = 128;
  @Help("Try to minimize sum of array lengths in generated test cases")
  public boolean ARRAY_LENGTH_MINIMIZE = true;  
  @Help("Factor for subsequent array length")
  public int ARRAY_LENGTH_MINIMIZE_FACTOR = 4;

  @Help("Artificial maximum number of local variables in a method/constructor")
  public int MAX_LOCALS_DEFAULT = 200;

  private int CONTEXT_HEIGHT_DSC_BASE = 0;

  @Help("Base context shared by all explorations. " +
  		"At this level, Dsc persists, for example, types.")
  public int CONTEXT_HEIGHT_BASE_OF_ALL_EXPLORATIONS =
  	CONTEXT_HEIGHT_DSC_BASE;

  @Help("Base context of a single method exploration. " +
  		"At this level, Dsc persists, for example, " +
  		"variables representing method parameters.")
  public int CONTEXT_HEIGHT_BASE_OF_SINGLE_METHOD_EXPLORATION =
  	CONTEXT_HEIGHT_BASE_OF_ALL_EXPLORATIONS + 1;


  public final String CLINIT    = "<clinit>"; //$NON-NLS-1$
  public final String   INIT    = "<init>"; //$NON-NLS-1$

  public final String FIELD_SEP  = "/"; //$NON-NLS-1$


  public final String ARRAY_VALUES_SUFFIX	= "#arrayValues"; //$NON-NLS-1$
  public final String ARRAY_LENGTH_SUFFIX	= "#arrayLength"; //$NON-NLS-1$
  public final String VAR_PREFIX 					= "#var"; //$NON-NLS-1$

  /**
   * Array containing the super-types of a mock class.
   */
  public final String SUPERTYPES_SUFFIX   = "#superTypes"; //$NON-NLS-1$

  /**
   * Super-class of a mock class.
   */
  public final String SUPER_CLASS_OF_MOCK_CLASS_SUFFIX	= "#superClassOfMockClass"; //$NON-NLS-1$

  public final String REF_DYNAMICTYPE  	= "#dynamicType"; //$NON-NLS-1$
  public final String DYNAMICTYPE_CLASSREF= "#classRef"; //$NON-NLS-1$
  public final String SUPERTYPES_VAR      = "#superTypes_var"; //$NON-NLS-1$
  public final String TYPE_ABSTRACT     	= "#isAbstract"; //$NON-NLS-1$
  public final String TYPE_ARRAY		     	= "#isArray"; //$NON-NLS-1$
  public final String TYPE_FINAL        	= "#isFinal"; //$NON-NLS-1$
  public final String TYPE_INTERFACE    	= "#isInterface"; //$NON-NLS-1$
  public final String TYPE_PUBLIC       	= "#isPublic"; //$NON-NLS-1$




  // TODO: Implement this, should be similar to running all Roops methods
  // @Help("Run on all public static methods declared by public classes")
  // public boolean RUN_ON_ALL_PUBLIC_PUBLIC_STATIC_METHODS = false;
  
  // TODO: Refactor the Roops flags
  // @Help("Run on all Roops benchmark methods")
  // public boolean RUN_ON_ALL_ROOPS_BENCHMARK_METHODS = false;



  @Help("include reference type constraints in generated constraint systems")
  public boolean TYPE_CONSTRAINTS = true;
  
  @Help("map each type to its subtypes, instead of to its super types")
  public boolean TYPE_MAPPED_TO_SUBTYPES = false;  

  /**
   * TODO: Get a more precise answer by calling getAllLoadedClasses() on the
   * java.lang.instrument.Instrumentation instance passed to our JvmAgent
   * and diffing it with the list of classes we transformed.
   *
   * @param typeName some/package/SomeType
   * @return if we omit typeName from instrumentation
   */
  public boolean isIgnored(String type) {
  	String typeName = type.replace("/", "."); //$NON-NLS-1$ //$NON-NLS-2$

    for (String prefix: DO_INSTRUMENT_PREFIXES) // positive list overrides exclusions
      if (typeName.startsWith(prefix))
        return false;

    for (String prefix: DO_NOT_INSTRUMENT_PREFIXES)
      if (typeName.startsWith(prefix))
        return true;

    return false;
  }



  /* LOG options */
  
  @Help("Directory to which Dsc writes its log files.")
  public String LOG_DIR_NAME = "log"; //$NON-NLS-1$
  
  @Help("log all constraints (branch conditions, type encoding, etc.) issued to Z3 to standard out.")
  public boolean LOG_ALL_CONSTRAINTS = false;

  @Help("log all constraints to file, write files to LOG_DIR_NAME.")
  public boolean LOG_ALL_CONSTRAINTS_TO_FILE = false;
  
  @Help("log separator for standard out.")
  public String LOG_SECTION_PREFIX = "-------------------- "; //$NON-NLS-1$
 
  @Help("log branch conditions issued to Z3 to standard out.")
  public boolean LOG_BRANCH_CONDITIONS = false;
  
  @Help("log each id Dsc assigns to each type to standard out.")
  public boolean LOG_TYPE_IDENTIFIERS = false;
  
  @Help("log type properties (super-types, abstract/public/etc.) to standard out.")
  public boolean LOG_TYPE_PROPERTIES = false;
  
  @Help("log type of variables etc. to standard out.")
  public boolean LOG_TYPE_OF_VALUES = false;
  
  @Help("log number of Dsc Ast nodes created")
	public boolean LOG_AST_COUNTS = true;

	@Help("log Z3 Ast creation")
	public boolean LOG_AST_Z3 = false;

	/* Logging branch ids */
	@Help("log branch id")
	public String LOG_BRANCH_FILE_NAME = "C:\\BranchOutput.txt"; //$NON-NLS-1$

	/**
	 * For example, for an axiom <code>forall x: x.length > 0</code>,
	 * DSC may assert instances<ul><li>
	 *   <code>arr.length > 0</code></li><li>
	 *   <code>c.length > 0</code></li><li>
	 *   etc.</li></ul>
	 */
	@Help("Log every concrete (asserted) instance of a (universally quantified) " +
	"background axiom that pertains to arrays.")
	public boolean LOG_BACKGROUND_AXIOM_INSTANCES_ARRAYS = false;

	@Help("log each executed bytecode instruction")
	public boolean LOG_BYTECODE = false;

	@Help("log each user class initialization")
	public boolean LOG_CLINIT = false;

	@Help("log stack trace of any exceptions during execution")
	public boolean LOG_EXCEPTIONS = false;

	@Help("log path condition and result of each execution")
	public boolean LOG_EXEC_RESULTS = false;

	@Help("log Dsc exploration: push/pop.")
	public boolean LOG_EXPLORATION = false;

	@Help("log input values (variable assignments) used for each execution")
	public boolean LOG_INPUT = false;

  @Help("log Dsc exploration: malloc/persist/assert. -- broken.")
  public boolean LOG_MALLOC_PERSIST_DISCARD = false;


	@Help("log number of times we ask Z3 to check SAT of a constraint system, " +
  		"how many of those Z3 found a model for, and how many of those lead " +
  		"to a new execution path.")
  public boolean LOG_MODEL_COUNTS = true;

  @Help("log each solution in Dsc notation")
	public boolean LOG_MODEL_DSC = false;

  @Help("log each solution in the Z3 notation")
  public boolean LOG_MODEL_Z3 = false;

  @Help("log path condition on null check")
  public boolean LOG_NULL_CHECKS = false;

//  @Help("log trivially true conjuncts (x==x) etc. when logging the path condition in Dsc notation")
//  public boolean LOG_PATH_COND_DSC_TRIVIAL_TRUE = false;

  @Help("log (not (x==null)) conjuncts when logging the path condition in Dsc notation")
  public boolean LOG_PATH_COND_DSC_NOT_NULL = true;

  @Help("File name for Roops output [Mainul]")
  public String LOG_ROOPS_FILE_NAME = "c:\\DscOutput.txt"; //$NON-NLS-1$

  @Help("log Roops output to file")
  public boolean LOG_ROOPS_TO_FILE = false;

  @Help("log summary of the entire exploration")
	public boolean LOG_SUMMARY = true;

	@Help("log instance fields with summary.")
  public boolean LOG_SUMMARY_INSTANCE_FIELDS = false;

  @Help("log Z3 execution times")
  public boolean LOG_Z3_TIMES = false;

  @Help("Filename for Z3 log, write file to LOG_DIR_NAME")
  public String LOG_Z3_TRACE_FILE_NAME = 
    LOG_DIR_NAME+Log.FS+"z3-"+System.currentTimeMillis()+".out";  //$NON-NLS-1$ //$NON-NLS-2$

  @Help("Make Z3 log into a text file")
  public boolean LOG_Z3_TRACE_TO_FILE = false;
  
  @Help("Log Z3 trace messages to standard error")
  public boolean LOG_Z3_TRACE_TO_STD_ERR = false;

  
  
  /* Convention: We represent a Z3 parameter PPP with as Z3PARAM_PPP */

  @Help("Approximate timeout for each solver query (milliseconds), 0 = no timeout.")
  public int Z3PARAM_SOFT_TIMEOUT = 5000;

  @Help("Model construction.")
  public boolean Z3PARAM_MODEL = true;

//  @Help("Quantifier.")
//  public boolean Z3PARAM_MBQI = true;
//  
//  @Help("Quantifier.")
//  public boolean Z3PARAM_MACRO_FINDER = true;

  /*
   * Z3 configuration parameters.<br><br>
   *
   * The parameters (together with their default values and description) are
   * <b>copied verbatim from the output of Z3 version 1.3</b>:
   * <pre>
   *   z3.exe -ini?
   * </pre>
   *
   * You have to set these flags before creating a Z3 context, otherwise
   * your settings will be ignored.
   */
//  Z3PARAM_ARITH_RANDOM_SEED(0),             // random seed for the simplex module.
//  Z3PARAM_ARITH_SOLVER(2),                  // arithmetic solver: 0 - no solver, 1 - bellman-ford based solver (diff. logic only), 2 - simplex based solver, 3 - floyd-warshall based solver (diff. logic only) and no theory combination.
//  Z3PARAM_Z3PARAM_CLAUSE_DELETION_FREQUENCY(5000),  // frequency (number of conflicts) of periodic deletion of not useful lemmas.
//  Z3PARAM_INITIAL_RESTART_FREQUENCY(100),   // inital restart frequency in number of conflicts.
//  Z3PARAM_MAM(4),                           // matching abstract machine to be used: 0 - eager instantiation, 1 - for debugging purposes, 2 - for debugging purposes, 3 - lazy instantiation, 4 - hybrid (eager/lazy) instantiation, 5 - robust.
//  Z3PARAM_MAX_ACKERMANN_CLAUSES(1000),      // maximum number of ackermann clauses that can be created.
//  Z3PARAM_MAX_COUNTEREXAMPLES(1),           // maximum number of counterexamples when using Simplify front end.
//  Z3PARAM_MAX_EXTRA_MULTI_PATTERNS(0),      // when patterns are not provided, the prover uses a heuristic to infer them. This option sets the threshold on the number of extra multi-patterns that can be created. By default, the prover creates at most one multi-pattern when there is no unary pattern.
//  Z3PARAM_PHASE_SELECTION(0),               // phase selection heuristic: 0 - always false, 1 - always true, 2 - maximises propagation, 3 - maximises number of satisfied clauses, 4 - phase caching.
//  Z3PARAM_PROFILE_QUANTIFIERS_FREQ(2147483647), //how often profile messages are displayed (in number of instances) (used only when PROFILE_QUANTIFIERS is true).
//  Z3PARAM_QUANT_EAGER_MAX_DEPTH(10),        // maximum matching depth for eager instantiation.
//  Z3PARAM_QUANT_IREL_MAX_DEPTH(0),          // maximum matching depth for irrelevant terms, the default behavior is to ignore irrelevant terms during quantifier instantiation.
//  Z3PARAM_QUANT_LAZY_MAX_DEPTH(20),         // maximum matching depth for lazy instantiation.
//  //Z3PARAM_RANDOM_CASE_SPLIT_FREQ: percentage, default: 0.01, frequency of random case splits.
//  Z3PARAM_RELEVANCY(2),                     // relevancy propagation heuristic: 0 - disabled, 1 - relevancy is tracked by only affects quantifier instantiation, 2 - relevancy is tracked, and an atom is only asserted if it is relevant.
//  //Z3PARAM_RESTART_FREQUENCY_INCREMENT: double, default: 1.5, after every restart, the restart frequency is multiplied by RESTART_FREQUENCY_INCREMENT.
//  Z3PARAM_SAT_RANDOM_SEED(0),               // random seed for the Z3 SAT solver.
//  Z3PARAM_THRESHOLD_FOR_ACKERMANN(10),      // number of times the congruence rule must be used before Leibniz's rule is expanded.
//  Z3PARAM_VERBOSE(0);                       // be verbose, where the value is the verbosity level.
//  Z3PARAM_AT_LABELS_CEX(false),           // only use labels that contain '@' when building multiple counterexamples.
//  Z3PARAM_AUTO_CONFIG(true),              // use heuristics to set Z3 configuration parameters, it is only available for the SMT-LIB input format.
//  Z3PARAM_BLOCK_LOOOP_PATTERNS(true),     // block looping patterns during pattern inference.
//  Z3PARAM_CONTEXT_SIMPLIFY(false),        // simplify formula using contextual rewriting.
//  Z3PARAM_DEFAULT_QID(false),             // create a default quantifier id based on its position, the id is used to report profiling information (see PROFILE_QUANTIFIERS).
//  Z3PARAM_DELAY_NEW_CASE_SPLITS(true),    // delay case-splits (i.e., boolean variables) created during backtracking search.
//  Z3PARAM_DELETE_IRRELEVANT_CLAUSES(true),// periodic deletion of not useful lemmas.
//  Z3PARAM_DISEQ_TH_PROPAGATION(true),     // disequality propagation.
//  Z3PARAM_DYNAMIC_ACKERMANN(false),       // dynamic ackermannization.
//  Z3PARAM_ELIM_TERM_ITE(false),           // eliminate term if-then-else in the preprocessor.
//  Z3PARAM_EQ_TH_PROPAGATION(true),        // equality propagation.
//  Z3PARAM_EXPENSIVE_CLAUSE_MINIMIZATION(true),    // lemma minimization algorithm.
//  Z3PARAM_FOURIER_MOTZKIN_ELIM(false),    // eliminate quantified variables using Fourier Motzkin elimination.
//  Z3PARAM_HIDE_UNUSED_PARTITIONS(true),   // hide unused partitions, some partitions are associated with internal terms/formulas created by Z3.
//  Z3PARAM_INTERACTIVE(false),             // interactive mode using Simplify input format.
//  Z3PARAM_MODEL_VALUE_COMPLETION(true),   // associate a value with each partition, in the (untyped) Simplify front-end, Z3 internally assumes that everything is an integer, so it may be convenient to use MODEL_VALUE_COMPLETION=false, this option must not be used from the C and Managed APIs.
//  Z3PARAM_PARTIAL_MODELS(false),          // partial function interpretations.
//  Z3PARAM_PROFILE_QUANTIFIERS(false),     // profile quantifier instantiation.
//  Z3PARAM_QUANT_BOUND_INST(false),        // quantifier instantiation using bounds of integer/real variables, this heuristic is very expensive, but it allows Z3 to prove some properties that it would fail otherwise.
//  Z3PARAM_REFLECT_BV_OPS(false),          // reflect theory operators in the egraph, this feature allows bit-vector operators to be used as patterns, and (in some cases) it improves the performance of benchmarks that contains bit-vectors and quantifiers.
//  Z3PARAM_SIMPLIFY_CLAUSES(true),         // periodic clause database simplification.
//  Z3PARAM_STATISTICS(false),              // display statistics.
//  Z3PARAM_TYPE_CHECK(true),               // type checker.
//  Z3PARAM_VALIDATE_MODEL(false),          // validate the model.
//  Z3PARAM_Z3_SOLVER_LL_PP(false),         // pretty print asserted constraints using low-level printer (Z3 input format specific).
//  Z3PARAM_Z3_SOLVER_SMT_PP(false);        // pretty print asserted constraints using SMT printer (Z3 input format specific).
  /* TODO: Z3 says "Error setting 'STATISTICS', reason: unknown parameter."  */
//,set(STATISTICS, true)
  /* TODO: Z3 says "Error setting 'VERBOSE', reason: unknown parameter." */
//set(VERBOSE, 1)
/* TODO: Following does not seem to work.
 * Because of Smt-Lib benchmark format? */
//,set(PARTIAL_MODELS, true)

/* TODO: Pretty printint in SMT format does not seem to work */
//,set(Z3_SOLVER_SMT_PP, true)


  /**
   * Common (but not universal) return code convention for successful
   * program completion.
   */
  public final int EXIT_SUCCESS = 0;
  public final int EXIT_FAILURE = 1;

  public final String[] HELP_PARAMS = {"--help", "-h", "/h"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

  /**
   * Print all options and their current values
   */
  String print() 
  {
    final StringBuilder sb = new StringBuilder(10000);
    Field[] fields = singleton.getClass().getFields();
    for (Field field: fields) 
    {
      if (Modifier.isFinal(field.getModifiers()))
        continue;   // skip final fields
      if (field.getAnnotation(Hide.class) != null)
        continue;   // skip "hidden" fields
      int nameLength = field.getName().length();
      
      sb.append("  "); //$NON-NLS-1$
      sb.append(field.getName());
      sb.append(" = "); //$NON-NLS-1$
      try {
        sb.append(field.get(singleton));
      }
      catch (IllegalAccessException iae) {
        iae.printStackTrace();
      }
      final Help help = field.getAnnotation(Help.class);
			if (help != null) {
				if (nameLength<14)
					sb.append("\t");	 //$NON-NLS-1$ // TODO: better formatting

				sb.append("\t-- "); //$NON-NLS-1$
				sb.append(help.value());
			}
			sb.append(Log.NL);
    }
    return sb.toString();
  }
  
  private static final String dl = "<dl>"; //$NON-NLS-1$
  private static final String _dl = "</dl>"; //$NON-NLS-1$
  private static final String dt = "<dt>"; //$NON-NLS-1$
  private static final String _dt = "</dt>"; //$NON-NLS-1$
  private static final String dd = "<dd>"; //$NON-NLS-1$
  private static final String _dd = "</dd>"; //$NON-NLS-1$  
  
  String printHtml() 
  {
    final StringBuilder sb = new StringBuilder(10000).append(dl).append(Log.NL);
    final Field[] fields = singleton.getClass().getFields();
    for (Field field: fields) 
    {
      if (Modifier.isFinal(field.getModifiers()))
        continue;   // skip final fields
      if (field.getAnnotation(Hide.class) != null)
        continue;
      sb.append(dt);
      sb.append(field.getName()).append(" = "); //$NON-NLS-1$
      try {
        sb.append(field.get(singleton));
      }
      catch (IllegalAccessException iae) {
        iae.printStackTrace();
      }
      sb.append(_dt);
      final Help help = field.getAnnotation(Help.class);
      if (help != null) 
        sb.append(dd).append(help.value()).append(_dd);
      sb.append(Log.NL);
    }
    return sb.append(_dl).toString();
  }  
  

  /**
   * Emit configuration options together with their default values
   * as html.
   */
  public static void main(String[] args)
  {
    MainConfig conf = setInstance();
    System.out.println(conf.printHtml());
  }
}

