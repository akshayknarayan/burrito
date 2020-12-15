mod f {
    #[bertha_attr::chunnel_semantics]
    pub struct Bar;
}

#[bertha_attr::chunnel_semantics]
struct Foo;

mod g {
    #[bertha_attr::chunnel_semantics]
    pub struct Baz;
}

#[test]
fn main() {
    let _ = Foo;
    let _ = f::Bar;
}
