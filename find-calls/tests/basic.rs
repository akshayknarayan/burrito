use find_calls::*;

#[find_calls]
pub struct MyThingy;

#[test]
fn main() {
    let t = MyThingy;
}
