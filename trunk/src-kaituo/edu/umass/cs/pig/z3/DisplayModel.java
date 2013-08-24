package edu.umass.cs.pig.z3;

import edu.umass.cs.pig.MainConfig;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_func_decl;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_model;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_symbol;
import edu.umass.cs.z3.Z3_lbool;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_app;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

public class DisplayModel {
	Z3Context z3;

	public DisplayModel() {
		MainConfig.setInstance();
		z3 = Z3Context.get();
	}

	public void display_model(SWIGTYPE_p__Z3_model m) {
		long num_constants;
		long i;

		num_constants = z3.get_model_num_constants(m);
		for (i = 0; i < num_constants; i++) {
			SWIGTYPE_p__Z3_symbol name;
			SWIGTYPE_p__Z3_func_decl cnst = z3.get_model_constant(m, i);
			SWIGTYPE_p__Z3_ast a, v, o;
			// Z3_lbool ok;
			name = z3.get_decl_name(cnst);
			display_symbol(name);
			System.out.print(" = ");
			a = z3.mk_app(cnst, 0, null);
			v = a;
			o = z3.eval(m, a);
			display_ast(v);
			display_ast(o);
			System.out.println();
		}
		display_function_interpretations(m);
	}

	public void display_function_interpretations(SWIGTYPE_p__Z3_model m) {
		long num_functions, i;

		System.out.print("function interpretations:\n");

		num_functions = z3.get_model_num_funcs(m);
		for (i = 0; i < num_functions; i++) {
			SWIGTYPE_p__Z3_func_decl fdecl;
			SWIGTYPE_p__Z3_symbol name;
			SWIGTYPE_p__Z3_ast func_else;
			long num_entries, j;

			fdecl = z3.get_model_func_decl(m, i);
			name = z3.get_decl_name(fdecl);
			display_symbol(name);
			System.out.print(" = {");
			num_entries = z3.get_model_func_num_entries(m, i);
			for (j = 0; j < num_entries; j++) {
				long num_args, k;
				if (j > 0) {
					System.out.print(", ");
				}
				num_args = z3.get_model_func_entry_num_args(m, i, j);
				System.out.print("(");
				for (k = 0; k < num_args; k++) {
					if (k > 0) {
						System.out.print(", ");
					}
//					System.out.print(z3.get_model_func_entry_arg(m, i, j, k));
					display_ast(z3.get_model_func_entry_arg(m, i, j, k));
				}
				System.out.print("|->");
				display_ast(z3.get_model_func_entry_value(m, i, j));
				System.out.print(")");
			}
			if (num_entries > 0) {
				System.out.print(", ");
			}
			System.out.print("(else|->");
			func_else = z3.get_model_func_else(m, i);
//			System.out.print(func_else);
			display_ast(func_else);
			System.out.print(")}\n");
		}
	}

	void display_symbol(SWIGTYPE_p__Z3_symbol s) {
		switch (z3.get_symbol_kind(s)) {
		case Z3_INT_SYMBOL:
			System.out.print(z3.get_symbol_int(s));
			break;
		case Z3_STRING_SYMBOL:
			System.out.print(z3.get_symbol_string(s));
			break;
		default:
			System.err.println("unreachable");
		}
	}

	public void display_ast(SWIGTYPE_p__Z3_ast v) {
		switch (z3.get_ast_kind(v)) {
		case Z3_NUMERAL_AST: {
			SWIGTYPE_p__Z3_sort t;
			System.out.print(z3.get_numeral_string(v));
			t = z3.get_sort(v);
			System.out.print(":");
			display_sort(t);
			break;
		}
		case Z3_APP_AST: {
			long i;
			SWIGTYPE_p__Z3_app app = z3.Z3_to_app(v);
			long num_fields = z3.get_app_num_args(app);
			SWIGTYPE_p__Z3_func_decl d = z3.get_app_decl(app);
			System.out.print(z3.func_decl_to_string(d));
			if (num_fields > 0) {
				System.out.print("[");
				for (i = 0; i < num_fields; i++) {
					if (i > 0) {
						System.out.print(", ");
					}
					display_ast(z3.get_app_arg(app, i));
				}
				System.out.print("]");
			}
			break;
		}
		case Z3_QUANTIFIER_AST: {
			System.out.print("quantifier");
			;
		}
		default:
			System.out.print("#unknown");
		}
	}

	void display_sort(SWIGTYPE_p__Z3_sort ty) {
		switch (z3.get_sort_kind(ty)) {
		case Z3_UNINTERPRETED_SORT:
			display_symbol(z3.get_sort_name(ty));
			break;
		case Z3_BOOL_SORT:
			System.out.print("bool");
			break;
		case Z3_INT_SORT:
			System.out.print("int");
			break;
		case Z3_REAL_SORT:
			System.out.print("real");
			break;
		case Z3_BV_SORT:
			System.out.print("bv" + z3.get_bv_sort_size(ty));
			break;
		case Z3_ARRAY_SORT:
			System.out.print("[");
			display_sort(z3.get_array_sort_domain(ty));
			System.out.print("->");
			display_sort(z3.get_array_sort_range(ty));
			System.out.print("]");
			break;
		case Z3_DATATYPE_SORT:
			if (z3.get_datatype_sort_num_constructors(ty) != 1) {
				System.out.print(z3.sort_to_string(ty));
				break;
			}
			{
				long num_fields = z3.get_tuple_sort_num_fields(ty);
				long i;
				System.out.print("(");
				for (i = 0; i < num_fields; i++) {
					SWIGTYPE_p__Z3_func_decl field = z3
							.get_tuple_sort_field_decl(ty, i);
					if (i > 0) {
						System.out.print(", ");
					}
					display_sort(z3.get_range(field));
				}
				System.out.print(")");
				break;
			}
		default:
			System.out.print("unknown[");
			display_symbol(z3.get_sort_name(ty));
			System.out.print("]");
			break;
		}
	}
	
	

}
