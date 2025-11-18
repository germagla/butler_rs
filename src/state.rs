#[derive(Default)]
pub struct BotState {
    // Add shared state here later
}

impl BotState {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}
