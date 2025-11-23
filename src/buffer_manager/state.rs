/// Simple enum used by higher-level code to select fix mode.
#[derive(Clone, Copy, Debug)]
pub enum FixMode {
    Shared,
    Exclusive,
}
