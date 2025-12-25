//! Logging functionality for `sqlite_wasm_reader`

use std::str::FromStr;
use std::sync::Mutex;

static LOGGER: Mutex<Option<Logger>> = Mutex::new(None);

/// Log levels in order of increasing verbosity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum LogLevel {
    /// Only critical errors that prevent operation
    Error = 0,
    /// Important warnings and errors
    Warn = 1,
    /// General information about operations
    Info = 2,
    /// Detailed debugging information
    Debug = 3,
    /// Very detailed tracing information
    Trace = 4,
}

impl LogLevel {
    /// Get the default log level
    #[must_use]
    pub const fn default() -> Self {
        Self::Info
    }
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "warn" | "warning" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err(format!("Invalid log level: {s}")),
        }
    }
}

/// Logger implementation
#[derive(Clone)]
pub struct Logger {
    level: LogLevel,
}

impl Logger {
    /// Create a new logger with the specified level
    #[must_use]
    pub const fn new(level: LogLevel) -> Self {
        Self { level }
    }

    /// Create a logger with default level (Info)
    #[must_use]
    pub const fn default() -> Self {
        Self::new(LogLevel::default())
    }

    /// Set the log level
    pub const fn set_level(&mut self, level: LogLevel) {
        self.level = level;
    }

    /// Get the current log level
    #[must_use]
    pub const fn level(&self) -> LogLevel {
        self.level
    }

    /// Log a message if the level is enabled
    pub fn log(&self, level: LogLevel, message: &str) {
        if level <= self.level {
            let prefix = match level {
                LogLevel::Error => "ERROR",
                LogLevel::Warn => "WARN",
                LogLevel::Info => "INFO",
                LogLevel::Debug => "DEBUG",
                LogLevel::Trace => "TRACE",
            };
            eprintln!("[sqlite_wasm_reader] {prefix}: {message}");
        }
    }

    /// Log an error message
    pub fn error(&self, message: &str) {
        self.log(LogLevel::Error, message);
    }

    /// Log a warning message
    pub fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message);
    }

    /// Log an info message
    pub fn info(&self, message: &str) {
        self.log(LogLevel::Info, message);
    }

    /// Log a debug message
    pub fn debug(&self, message: &str) {
        self.log(LogLevel::Debug, message);
    }

    /// Log a trace message
    pub fn trace(&self, message: &str) {
        self.log(LogLevel::Trace, message);
    }
}

/// Initialize the global logger
pub fn init_logger(level: LogLevel) {
    let mut guard = LOGGER.lock().unwrap();
    *guard = Some(Logger::new(level));
}

/// Initialize the global logger with default level (Info)
pub fn init_default_logger() {
    init_logger(LogLevel::default());
}

/// Get the global logger instance
pub fn get_logger() -> Logger {
    let guard = LOGGER.lock().unwrap();
    if let Some(ref logger) = *guard {
        logger.clone()
    } else {
        drop(guard);
        init_default_logger();
        let guard = LOGGER.lock().unwrap();
        guard.as_ref().unwrap().clone()
    }
}

/// Set the global log level
pub fn set_log_level(level: LogLevel) {
    let mut guard = LOGGER.lock().unwrap();
    if let Some(ref mut logger) = *guard {
        logger.set_level(level);
    } else {
        *guard = Some(Logger::new(level));
    }
}

/// Convenience functions for global logging
pub fn log_error(message: &str) {
    get_logger().error(message);
}

pub fn log_warn(message: &str) {
    get_logger().warn(message);
}

pub fn log_info(message: &str) {
    get_logger().info(message);
}

pub fn log_debug(message: &str) {
    get_logger().debug(message);
}

pub fn log_trace(message: &str) {
    get_logger().trace(message);
}

/// Check if a log level is enabled
#[must_use]
pub fn is_enabled(level: LogLevel) -> bool {
    level <= get_logger().level()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_creation() {
        assert_eq!(LogLevel::Error as u8, 0);
        assert_eq!(LogLevel::Warn as u8, 1);
        assert_eq!(LogLevel::Info as u8, 2);
        assert_eq!(LogLevel::Debug as u8, 3);
        assert_eq!(LogLevel::Trace as u8, 4);
    }

    #[test]
    fn test_log_level_default() {
        assert_eq!(LogLevel::default(), LogLevel::Info);
    }

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from_str("error"), Ok(LogLevel::Error));
        assert_eq!(LogLevel::from_str("warn"), Ok(LogLevel::Warn));
        assert_eq!(LogLevel::from_str("warning"), Ok(LogLevel::Warn));
        assert_eq!(LogLevel::from_str("info"), Ok(LogLevel::Info));
        assert_eq!(LogLevel::from_str("debug"), Ok(LogLevel::Debug));
        assert_eq!(LogLevel::from_str("trace"), Ok(LogLevel::Trace));

        // Case insensitive
        assert_eq!(LogLevel::from_str("ERROR"), Ok(LogLevel::Error));
        assert_eq!(LogLevel::from_str("Debug"), Ok(LogLevel::Debug));

        // Invalid values
        assert!(LogLevel::from_str("invalid").is_err());
        assert!(LogLevel::from_str("").is_err());
    }

    #[test]
    fn test_log_level_comparison() {
        assert!(LogLevel::Error < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Trace);

        assert!(LogLevel::Trace > LogLevel::Debug);
        assert!(LogLevel::Debug > LogLevel::Info);
        assert!(LogLevel::Info > LogLevel::Warn);
        assert!(LogLevel::Warn > LogLevel::Error);
    }

    #[test]
    fn test_logger_creation() {
        let logger = Logger::new(LogLevel::Debug);
        assert_eq!(logger.level(), LogLevel::Debug);

        let default_logger = Logger::default();
        assert_eq!(default_logger.level(), LogLevel::Info);
    }

    #[test]
    fn test_logger_level_setting() {
        let mut logger = Logger::new(LogLevel::Info);
        assert_eq!(logger.level(), LogLevel::Info);

        logger.set_level(LogLevel::Debug);
        assert_eq!(logger.level(), LogLevel::Debug);
    }

    #[test]
    fn test_global_logger_initialization() {
        // Test that we can initialize the global logger
        init_default_logger();
        let logger = get_logger();
        assert_eq!(logger.level(), LogLevel::Info);
    }

    #[test]
    fn test_global_log_level_setting() {
        init_default_logger();
        set_log_level(LogLevel::Debug);
        let logger = get_logger();
        assert_eq!(logger.level(), LogLevel::Debug);
    }

    #[test]
    fn test_logging_functions() {
        init_default_logger();
        set_log_level(LogLevel::Debug);

        // These should not panic
        log_error("Test error message");
        log_warn("Test warning message");
        log_info("Test info message");
        log_debug("Test debug message");
        log_trace("Test trace message");
    }

    #[test]
    fn test_is_enabled() {
        init_default_logger();
        set_log_level(LogLevel::Info);

        assert!(is_enabled(LogLevel::Error));
        assert!(is_enabled(LogLevel::Warn));
        assert!(is_enabled(LogLevel::Info));
        assert!(!is_enabled(LogLevel::Debug));
        assert!(!is_enabled(LogLevel::Trace));

        set_log_level(LogLevel::Trace);
        assert!(is_enabled(LogLevel::Error));
        assert!(is_enabled(LogLevel::Warn));
        assert!(is_enabled(LogLevel::Info));
        assert!(is_enabled(LogLevel::Debug));
        assert!(is_enabled(LogLevel::Trace));
    }
}
