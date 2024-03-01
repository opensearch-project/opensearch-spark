package org.opensearch.flint.core.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CustomLogging class using the {@link CustomJsonLayout} for logging.
 */
public class CustomLogging {
    // Define a static logger variable so that it references the custom logger
    private static final Logger logger = LogManager.getLogger(CustomLogging.class);

    public static void logDebug(String message) {
        logger.debug(message);
    }

    public static void logInfo(String message) {
        logger.info(message);
    }

    public static void logWarning(String message) {
        logger.warn(message);
    }

    public static void logWarning(String message, Throwable e) {
        logger.warn(message, e);
    }

    public static void logError(String message) {
        logger.error(message);
    }

    public static void logError(String message, Throwable throwable) {
        logger.error(message, throwable);
    }
}