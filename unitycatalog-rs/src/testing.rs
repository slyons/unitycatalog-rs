#[cfg(test)]
pub mod test_utils {
    use futures_util::FutureExt;
    use port_scanner::request_open_port;
    use std::panic::AssertUnwindSafe;
    use std::{process::Stdio, thread, time::Duration};
    use tokio::process::Command;

    pub fn cleanup_user_model() -> Vec<(&'static str, &'static str)> {
        vec![
            (
                r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
                "PID",
            ),
            (r"\d{13}", "TIMESTAMP"),
        ]
    }

    pub async fn start_uc() -> (u16, String) {
        let port = request_open_port().expect("Unable to allocate unopen port");
        let port_str = format!("{}:8080", port);

        let docker_id = Command::new("docker")
            .stderr(Stdio::null())
            .stdout(Stdio::piped())
            //.stderr(Stdio::piped())
            //.stdout(Stdio::null())
            .stdin(Stdio::null())
            .args(["run", "-d", "--rm", "-p", &port_str, "slyons/unity-catalog"])
            .output()
            .await
            .expect("Could not start Docker container");
        let mut child =
            String::from_utf8(docker_id.stdout).expect("Could not read Docker container ID");
        let _ = child.split_off(5);
        thread::sleep(Duration::from_secs(5));
        (port, child)
    }

    pub async fn stop_uc(child_id: String) {
        Command::new("docker")
            .stderr(Stdio::null())
            .stdout(Stdio::piped())
            .stdin(Stdio::null())
            .args(["kill", &child_id])
            .output()
            .await
            .expect("Could not stop Docker container");
    }

    pub async fn test_with_uc<F, Fut, R>(callback: F) -> R
    where
        F: FnOnce(u16) -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        let (port, child) = start_uc().await;
        let res = AssertUnwindSafe(callback(port)).catch_unwind().await;
        stop_uc(child).await;
        res.expect("Error occurred during test function")
    }
}
