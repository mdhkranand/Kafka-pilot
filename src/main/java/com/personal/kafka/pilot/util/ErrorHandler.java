package com.personal.kafka.pilot.util;

import javafx.application.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized error handling utility for consistent error reporting
 */
public class ErrorHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(ErrorHandler.class);
    
    /**
     * Handle errors with consistent logging and UI feedback
     */
    public static void handleError(String context, Exception e, ConsoleLogger consoleLogger) {
        String errorMessage = context + ": " + e.getMessage();
        
        // Log the error
        logger.error(errorMessage, e);
        
        // Show error in console
        if (consoleLogger != null) {
            Platform.runLater(() -> consoleLogger.append("[ERROR] " + errorMessage));
        }
    }
    
    /**
     * Handle errors with context prefix
     */
    public static void handleError(String prefix, String context, Exception e, ConsoleLogger consoleLogger) {
        handleError("[" + prefix + "] " + context, e, consoleLogger);
    }
    
    /**
     * Handle errors for async operations
     */
    public static void handleAsyncError(String context, Exception e, ConsoleLogger consoleLogger) {
        Platform.runLater(() -> handleError(context, e, consoleLogger));
    }
    
    /**
     * Handle errors with custom status label update
     */
    public static void handleErrorWithStatus(String context, Exception e, ConsoleLogger consoleLogger, 
                                           javafx.scene.control.Label statusLabel, String statusText) {
        handleError(context, e, consoleLogger);
        if (statusLabel != null) {
            Platform.runLater(() -> statusLabel.setText(statusText));
        }
    }
    
    /**
     * Handle errors without console logging (just logging)
     */
    public static void logError(String context, Exception e) {
        String errorMessage = context + ": " + e.getMessage();
        logger.error(errorMessage, e);
    }
    
    /**
     * Handle warnings (less severe than errors)
     */
    public static void handleWarning(String context, Exception e, ConsoleLogger consoleLogger) {
        String warningMessage = context + ": " + e.getMessage();
        
        // Log the warning
        logger.warn(warningMessage, e);
        
        // Show warning in console
        if (consoleLogger != null) {
            Platform.runLater(() -> consoleLogger.append("[WARNING] " + warningMessage));
        }
    }
    
    /**
     * Functional interface for console logging
     */
    @FunctionalInterface
    public interface ConsoleLogger {
        void append(String message);
    }
}
