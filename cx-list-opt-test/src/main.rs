use bertha;
use cx_list_opt::cx_list_opt;

#[derive(Default, Clone, Copy)]
struct Foo;
#[derive(Default, Clone, Copy)]
struct NewFoo;
impl From<Foo> for NewFoo {
    fn from(_: Foo) -> NewFoo {
        NewFoo
    }
}
#[derive(Default, Clone, Copy)]
struct Bar;

#[cx_list_opt(* |> Foo => * |> NewFoo)]
trait Opt {
    type A;
    fn a(self) -> Self::A;
}

#[cx_list_opt(Bar |> * |> Foo => Bar |> * |> NewFoo)]
trait Opt2 {
    type A;
    fn a(self) -> Self::A;
}

#[cx_list_opt(* |> Foo[foo] => Bar[default] |> * |> NewFoo[from:foo])]
trait Opt3 {
    type A;
    fn a(self) -> Self::A;
}

//#[cx_list_opt(* |> A => Select(B |> * |> C, * |> A))]
//trait Opt2 {
//    type A;
//    fn a(self) -> Self::A;
//}

fn main() {
    println!("Hello, world!");
}
