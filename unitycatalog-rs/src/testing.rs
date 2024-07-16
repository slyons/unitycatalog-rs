#[cfg(test)]
pub fn cleanup_user_model() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
            "PID"
        ),
        (
            r"\d{13}",
            "TIMESTAMP"
        )
    ]
}