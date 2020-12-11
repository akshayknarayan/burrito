extern crate proc_macro;

use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro_attribute]
pub fn find_calls(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let output = item.clone();
    let item = parse_macro_input!(item as syn::Item);

    match item {
        syn::Item::Fn(item) => {
            for stmt in item.block.stmts {
                match stmt {
                    syn::Stmt::Semi(expr, _) | syn::Stmt::Expr(expr) => {
                        match expr {
                            syn::Expr::Call(expr) => {
                                // TODO
                                eprintln!("call: {:?}", expr);
                            }
                            syn::Expr::MethodCall(expr) => {
                                // TODO
                                eprintln!("method call: {:?}", expr);
                            }
                            _ => (),
                        }
                    }
                    syn::Stmt::Item(_item) => {
                        // TODO recurse into this item looking for more functions?
                        unimplemented!();
                    }
                    _ => (),
                }
            }
        }
        _ => {
            eprintln!("wrong item type");
        }
    }

    output
}
