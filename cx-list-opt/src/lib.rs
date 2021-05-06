extern crate proc_macro;

use nom::IResult;
use proc_macro2::TokenStream;
use syn::parse_macro_input;

/// Define some semantics and a specialized implementation for them.
#[proc_macro_attribute]
pub fn cx_list_opt(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // get the spec of the transform we want
    let attr = proc_macro2::TokenStream::from(attr);
    let attr_str = attr.to_string();
    let transform = parse_transform(&attr_str).unwrap().1;

    // now we need the trait name and its function so we can impl it
    let item2 = item.clone();
    let syn_item = parse_macro_input!(item2 as syn::Item);
    let trait_item = match syn_item {
        syn::Item::Trait(trait_item) => trait_item,
        _ => return item,
    };

    let comps = TraitComponents::get(trait_item);
    let tokens = transform.write_impl(comps);
    let item = proc_macro2::TokenStream::from(item);
    let tokens = quote::quote! {
        #item
        #tokens
    };
    tokens.into()
}

#[derive(Debug)]
struct Transform {
    left: Stack,
    right: Stack,
}

impl Transform {
    fn write_impl(self, comps: TraitComponents) -> TokenStream {
        let TraitComponents {
            name,
            ret_type,
            func,
        } = comps;
        let func_sig = func.sig;
        let ret_type_name = &ret_type.ident;

        let mut tokens = quote::quote! {};
        for i in 0..2 {
            let (impltypes, from_type) = self.left.get_type(i);
            let (_impltypes2, to_type) = self.right.get_type(i);

            let des = self.left.get_destructure(i);
            let res = self.right.get_restructure(i);

            tokens = quote::quote! {
                #tokens

                impl<#impltypes> #name for #from_type {
                    type #ret_type_name = #to_type;
                    #func_sig {
                        #des
                        #res
                    }
                }
            };
        }

        tokens
    }
}

#[derive(Debug)]
enum Stack {
    Leaf(Expr),
    List(Vec<Expr>),
    Select { left: Box<Stack>, right: Box<Stack> },
}

impl Stack {
    fn get_restructure(&self, max_level: u8) -> proc_macro2::TokenStream {
        match self {
            Stack::Leaf(Expr::Ident(_, varsp)) => match varsp {
                Var::CreateDefault => {
                    quote::quote! { Default::default() }
                }
                Var::FromName(n) => {
                    quote::quote! { #n.into() }
                }
                Var::StoreDefaultName => {
                    quote::quote! { v0.into }
                }
                _ => unreachable!(),
            },
            Stack::Leaf(Expr::Wildcard) => {
                let mut exp = None;
                for i in 0..max_level {
                    let varname: syn::Ident = syn::parse_str(&format!("t{}", i)).unwrap();
                    exp = Some(if let Some(e) = exp {
                        quote::quote! { bertha::CxList { head: #varname, tail: #e } }
                    } else {
                        quote::quote! { bertha::CxList { head: #varname, tail: bertha::CxNil } }
                    });
                }
                quote::quote! { let #exp = self; }
            }
            Stack::List(exprs) => {
                let mut exp = None;
                let mut elems = exprs.to_vec();
                elems.reverse();
                let mut val = 0;
                for elem in elems {
                    match elem {
                        Expr::Ident(_, varsp) => {
                            let varname = match varsp {
                                Var::StoreDefaultName => {
                                    let varname: syn::Ident =
                                        syn::parse_str(&format!("v{}", val)).unwrap();
                                    val += 1;
                                    quote::quote! { #varname.into() }
                                }
                                Var::CreateDefault => {
                                    quote::quote! { Default::default() }
                                }
                                Var::FromName(n) => {
                                    quote::quote! { #n.into() }
                                }
                                _ => unreachable!(),
                            };
                            exp = Some(if let Some(e) = exp {
                                quote::quote! { bertha::CxList { head: #varname, tail: #e  } }
                            } else {
                                quote::quote! { bertha::CxList { head: #varname, tail: bertha::CxNil  } }
                            });
                        }
                        Expr::Wildcard => {
                            for i in 0..max_level {
                                let varname: syn::Ident =
                                    syn::parse_str(&format!("t{}", i)).unwrap();
                                exp = Some(if let Some(e) = exp {
                                    quote::quote! { bertha::CxList { head: #varname, tail: #e } }
                                } else {
                                    quote::quote! { bertha::CxList { head: #varname, tail: bertha::CxNil } }
                                });
                            }
                        }
                    }
                }

                quote::quote! { #exp }
            }
            _ => todo!(),
        }
    }

    fn get_destructure(&self, max_level: u8) -> proc_macro2::TokenStream {
        match self {
            Stack::Leaf(Expr::Ident(_, varsp)) => match varsp {
                Var::StoreDefaultName => {
                    quote::quote! { let v0 = self; }
                }
                Var::StoreName(n) => {
                    quote::quote! { let #n = self; }
                }
                _ => unreachable!(),
            },
            Stack::Leaf(Expr::Wildcard) => {
                let mut exp = None;
                for i in 0..max_level {
                    let varname: syn::Ident = syn::parse_str(&format!("t{}", i)).unwrap();
                    exp = Some(if let Some(e) = exp {
                        quote::quote! { bertha::CxList { head: #varname, tail: #e } }
                    } else {
                        quote::quote! { bertha::CxList { head: #varname, tail: bertha::CxNil } }
                    });
                }
                quote::quote! { let #exp = self; }
            }
            Stack::List(exprs) => {
                let mut exp = None;
                let mut elems = exprs.to_vec();
                elems.reverse();
                let mut val = 0;
                for elem in elems {
                    match elem {
                        Expr::Ident(_, varsp) => {
                            let varname = match varsp {
                                Var::StoreDefaultName => {
                                    let varname: syn::Ident =
                                        syn::parse_str(&format!("v{}", val)).unwrap();
                                    val += 1;
                                    quote::quote! { #varname }
                                }
                                Var::StoreName(n) => {
                                    quote::quote! { #n }
                                }
                                _ => unreachable!(),
                            };
                            exp = Some(if let Some(e) = exp {
                                quote::quote! { bertha::CxList { head: #varname, tail: #e  } }
                            } else {
                                quote::quote! { bertha::CxList { head: #varname, tail: bertha::CxNil  } }
                            });
                        }
                        Expr::Wildcard => {
                            for i in 0..max_level {
                                let varname: syn::Ident =
                                    syn::parse_str(&format!("t{}", i)).unwrap();
                                exp = Some(if let Some(e) = exp {
                                    quote::quote! { bertha::CxList { head: #varname, tail: #e } }
                                } else {
                                    quote::quote! { bertha::CxList { head: #varname, tail: bertha::CxNil } }
                                });
                            }
                        }
                    }
                }

                quote::quote! { let #exp = self; }
            }
            _ => todo!(),
        }
    }

    fn get_type(&self, max_level: u8) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
        match self {
            Stack::Leaf(Expr::Ident(name, _)) => (quote::quote! {}, quote::quote! { #name }),
            x @ Stack::Leaf(Expr::Wildcard) => {
                if max_level == 0 {
                    (
                        quote::quote! { T0 },
                        quote::quote! { bertha::CxList<T0, bertha::CxNil> },
                    )
                } else {
                    let head_name = format!("T{}", max_level);
                    let head_name_ident: syn::Ident = syn::parse_str(&head_name).unwrap();
                    let (tail_name_types, tail_name) = x.get_type(max_level - 1);
                    (
                        quote::quote! { #head_name_ident, #tail_name_types },
                        quote::quote! { bertha::CxList<#head_name_ident, #tail_name> },
                    )
                }
            }
            Stack::List(exprs) => {
                fn recurse(
                    exprs: &[Expr],
                    max_level: u8,
                ) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
                    if exprs.len() == 0 {
                        panic!("need at least 1 list element");
                    }
                    if exprs.len() == 1 {
                        match &exprs[0] {
                            w @ Expr::Wildcard => {
                                return Stack::Leaf(w.clone()).get_type(max_level)
                            }
                            Expr::Ident(ref name, _) => {
                                return (
                                    quote::quote! {},
                                    quote::quote! { bertha::CxList<#name, bertha::CxNil> },
                                );
                            }
                        }
                    }

                    let (first, rest) = exprs.split_first().unwrap();
                    let mut rest_type = recurse(rest, max_level);
                    match first {
                        Expr::Wildcard => {
                            for i in 0..max_level {
                                let (deftypes, rtype) = rest_type;
                                let tname: syn::Ident = syn::parse_str(&format!("T{}", i)).unwrap();
                                rest_type = (
                                    quote::quote! { #tname, #deftypes },
                                    quote::quote! { bertha::CxList<#tname, #rtype>},
                                );
                            }

                            rest_type
                        }
                        Expr::Ident(name, _) => {
                            let (deftypes, rtype) = rest_type;
                            (deftypes, quote::quote! { bertha::CxList<#name, #rtype> })
                        }
                    }
                }

                recurse(&exprs, max_level)
            }
            Stack::Select { left, right } => {
                let (htypes, head_name) = left.get_type(max_level);
                let (ttypes, tail_name) = right.get_type(max_level);
                (
                    quote::quote! { #htypes, #ttypes },
                    quote::quote! { bertha::Select<#head_name, #tail_name> },
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Var {
    StoreDefaultName,
    StoreName(TokenStream),
    CreateDefault,
    FromName(TokenStream),
}

#[derive(Debug, Clone)]
enum Expr {
    Ident(TokenStream, Var),
    Wildcard,
}

use nom::{
    branch::alt,
    bytes::complete::{tag, take_till, take_until},
    combinator::{all_consuming, complete, opt},
    multi::separated_list1,
    sequence::{delimited, separated_pair, tuple},
};
fn parse_transform(attr_input: &str) -> IResult<&str, Transform> {
    let (remain, (left, right)) = separated_pair(left_stack, tag("=>"), right_stack)(attr_input)?;
    Ok((remain, Transform { left, right }))
}

fn left_stack(stack_input: &str) -> IResult<&str, Stack> {
    let (remain, this_stack) = take_until("=>")(stack_input)?;
    let this_stack = this_stack.trim_end();
    Ok((remain, all_consuming(complete(parse_stack))(this_stack)?.1))
}

fn right_stack(stack_input: &str) -> IResult<&str, Stack> {
    let this_stack = stack_input.trim_start();
    Ok(("", all_consuming(complete(parse_stack))(this_stack)?.1))
}

fn parse_stack(input: &str) -> IResult<&str, Stack> {
    fn expr(input: &str) -> IResult<&str, Stack> {
        let (remain, expr) = parse_expr(input)?;
        Ok((remain, Stack::Leaf(expr)))
    }

    fn list(input: &str) -> IResult<&str, Stack> {
        let (remain, exprs) =
            separated_list1(alt((tag("|>"), tag("| >"), tag(" | > "))), parse_expr)(input)?;
        Ok((remain, Stack::List(exprs)))
    }

    fn select(input: &str) -> IResult<&str, Stack> {
        fn parse_stack_left(input: &str) -> IResult<&str, Stack> {
            let (remain, this_stack) = take_until(",")(input)?;
            Ok((remain, all_consuming(complete(parse_stack))(this_stack)?.1))
        }

        fn parse_stack_right(input: &str) -> IResult<&str, Stack> {
            let (remain, this_stack) = take_until(")")(input)?;
            Ok((remain, all_consuming(complete(parse_stack))(this_stack)?.1))
        }

        let (remain, (left, _, _, _, right)) = delimited(
            alt((tag("Select("), tag("Select ("))),
            tuple((
                parse_stack_left,
                opt(tag(" ")),
                tag(","),
                opt(tag(" ")),
                parse_stack_right,
            )),
            tag(")"),
        )(input)?;
        Ok((
            remain,
            Stack::Select {
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    }

    alt((select, list, expr))(input)
}

fn parse_expr(input: &str) -> IResult<&str, Expr> {
    fn parse_wild(input: &str) -> IResult<&str, Expr> {
        let (remain, _) = tag("*")(input)?;
        Ok((remain, Expr::Wildcard))
    }

    fn parse_ident(input: &str) -> IResult<&str, Expr> {
        fn parse_var(input: &str) -> IResult<&str, Var> {
            let (remain, var_str) = delimited(tag("["), take_until("]"), tag("]"))(input)?;
            if var_str == "default" {
                Ok((remain, Var::CreateDefault))
            } else if var_str.starts_with("from :") {
                let var_ident = syn::parse_str(var_str.trim_start_matches("from :")).unwrap();
                Ok((remain, Var::FromName(var_ident)))
            } else {
                let var_ident = syn::parse_str(var_str).unwrap();
                Ok((remain, Var::StoreName(var_ident)))
            }
        }

        let (remain, (ident, _, var_spec, _)) = tuple((
            take_till(|c| c == ' ' || c == '['),
            opt(tag(" ")),
            opt(parse_var),
            opt(tag(" ")),
        ))(input)?;
        let ident = syn::parse_str(ident).unwrap();
        Ok((
            remain,
            Expr::Ident(ident, var_spec.unwrap_or(Var::StoreDefaultName)),
        ))
    }

    let (r, (_, e)) = tuple((opt(tag(" ")), alt((parse_wild, parse_ident))))(input)?;
    Ok((r, e))
}

#[derive(Debug)]
struct TraitComponents {
    name: syn::Ident,
    ret_type: syn::TraitItemType,
    func: syn::TraitItemMethod,
}

impl TraitComponents {
    fn get(item: syn::ItemTrait) -> Self {
        match item {
            syn::ItemTrait {
                ident: name, items, ..
            } => {
                let rt = items[0].clone();
                let fm = items[1].clone();
                match (rt, fm) {
                    (syn::TraitItem::Type(ret_type), syn::TraitItem::Method(first_method)) => {
                        TraitComponents {
                            name,
                            ret_type,
                            func: first_method,
                        }
                    }
                    _ => {
                        panic!("Trait impl does not match macro");
                    }
                }
            }
        }
    }
}
