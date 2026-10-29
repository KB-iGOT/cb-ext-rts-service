package com.igot.cb;

/** A request that cannot be done; becomes "<EVENT>_RESULT" with status ERROR and this code. */
public class RtsError extends RuntimeException {

    final String code;

    RtsError(String code, String message) {
        super(message);
        this.code = code;
    }
}
