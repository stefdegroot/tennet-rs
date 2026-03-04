
pub fn default_to_zero_option (option: Option<String>) -> Option<f32> {
    if let Some(string) = option {
       string.parse().ok()
    } else {
        None
    }
}

pub fn default_string_to_zero (string: String) -> f32 {
    string.parse().unwrap_or(0.0)
}

pub fn default_some_string_to_zero (option: Option<String>) -> f32 {
    if let Some(n) = option {
        n.parse().unwrap()
    } else {
        0.0
    }
}