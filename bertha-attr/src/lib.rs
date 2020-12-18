extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use std::path::Path;
use syn::{parse_macro_input, spanned::Spanned};

static STATE_PATH: &'static str = "/tmp/bertha-attr/";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Semantic {
    kind: String,
    name: String,
    implementation: String,
}

/// Define some semantics and a specialized implementation for them.
#[proc_macro_attribute]
pub fn chunnel_semantics(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as syn::Expr);

    match attr {
        syn::Expr::Tuple(attr) if attr.elems.len() == 3 => {
            let kind = match attr.elems[0] {
                syn::Expr::Path(ref path) => ident_from_path(path),
                _ => unreachable!(),
            };

            let name = match attr.elems[1] {
                syn::Expr::Path(ref path) => ident_from_path(path),
                _ => unreachable!(),
            };

            let imp = attr.elems[2].clone();

            let sem = Semantic {
                kind: kind.to_string(),
                name: name.to_string(),
                implementation: quote!(#imp).to_string(),
            };

            push_entry(sem);
        }
        x => {
            return TokenStream::from(quote_spanned! {
                x.span() => compile_error!("chunnel_semantics attribute expects 3-element tuple")
            });
        }
    }

    item
}

/// Look up and insert a specialized implementation given the semantics.
#[proc_macro]
pub fn pick_semantics(arg: TokenStream) -> TokenStream {
    let arg = parse_macro_input!(arg as syn::Expr);
    let (kind, name) = match arg {
        syn::Expr::Tuple(attr) if attr.elems.len() == 2 => {
            let kind = match attr.elems[0] {
                syn::Expr::Path(ref path) => ident_from_path(path),
                _ => unreachable!(),
            };

            let name = match attr.elems[1] {
                syn::Expr::Path(ref path) => ident_from_path(path),
                _ => unreachable!(),
            };

            (kind, name)
        }
        x => {
            return TokenStream::from(quote_spanned! {
                x.span() => compile_error!("pick_semantics call expects 2-element tuple")
            });
        }
    };

    let kind_str = kind.to_string();
    let ents = get_entries(&kind_str);

    let ents: std::collections::HashMap<_, _> = ents
        .into_iter()
        .filter_map(|e| {
            if e.kind != kind_str {
                None
            } else {
                Some((e.name, e.implementation))
            }
        })
        .collect();

    let name_str = name.to_string();
    let imp = if ents.contains_key(&name_str) {
        ents.get(&name_str).unwrap()
    } else if ents.contains_key("Generic") {
        ents.get("Generic").unwrap()
    } else {
        return TokenStream::from(quote_spanned! {
            name.span() => compile_error!("could not find matching semantics")
        });
    };

    imp.parse().unwrap()
}

fn ident_from_path(expr: &syn::ExprPath) -> syn::Ident {
    expr.path.segments[0].ident.clone()
}

fn push_entry(s: Semantic) {
    let mut ents = get_entries(&s.kind);
    let filename = format!("{}.bertha", s.kind.clone());
    ents.push(s);

    let ser = bincode::serialize(&ents).unwrap();
    let p = Path::new(STATE_PATH).join(filename);
    std::fs::write(&p, &ser).unwrap();
}

fn get_entries(kind: &str) -> Vec<Semantic> {
    let filename = format!("{}.bertha", kind);
    std::fs::create_dir_all(Path::new(STATE_PATH)).unwrap();
    let p = Path::new(STATE_PATH).join(filename);
    if let Ok(f) = std::fs::read(&p) {
        bincode::deserialize(&f).unwrap()
    } else {
        Vec::new()
    }
}
