package io.absurd.sdk;

/**
 * Base exception class for all Absurd SDK exceptions.
 */
public class AbsurdException extends RuntimeException {
    
    public AbsurdException(String message) {
        super(message);
    }
    
    public AbsurdException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public AbsurdException(Throwable cause) {
        super(cause);
    }
}
