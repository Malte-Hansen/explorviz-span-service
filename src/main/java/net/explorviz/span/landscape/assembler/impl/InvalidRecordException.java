package net.explorviz.span.landscape.assembler.impl;

import net.explorviz.span.landscape.assembler.LandscapeAssemblyException;

import java.io.Serial;

/**
 * Thrown if a records that is to be inserted into the model is invalid.
 */
public class InvalidRecordException extends LandscapeAssemblyException {
    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidRecordException() {
    }

    public InvalidRecordException(final String message) {
        super(message);
    }
}
