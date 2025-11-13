use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone)]
#[allow(dead_code)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            max_retries: 3,
            delay_ms: 1000,
        }
    }
}
#[allow(dead_code)]
pub fn is_nonce_error(error_msg: &str) -> bool {
    let error_lower = error_msg.to_lowercase();
    error_lower.contains("invalid nonce")
        || error_lower.contains("nonce too low")
        || error_lower.contains("nonce conflict")
        || error_lower.contains("nonce too high")
        || error_lower.contains("replacement transaction underpriced")
}

#[allow(dead_code)]
pub async fn retry_on_nonce_error<F, Fut, T, E>(
    mut operation: F,
    config: RetryConfig,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempts = 0;
    loop {
        attempts += 1;
        match operation().await {
            Ok(result) => {
                return Ok(result);
            }
            Err(e) => {
                let error_msg = e.to_string();
                if is_nonce_error(&error_msg) {
                    if attempts >= config.max_retries {
                        eprintln!(
                            "❌ Max retries ({}) reached for nonce error: {}",
                            config.max_retries, error_msg
                        );
                        return Err(e);
                    }
                    let delay = Duration::from_millis(config.delay_ms);
                    eprintln!(
                        "⚠️  Nonce error detected (attempt {}/{}): {}",
                        attempts, config.max_retries, error_msg
                    );
                    eprintln!("   Retrying in {:?}...", delay);
                    sleep(delay).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
}
