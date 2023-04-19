package net.explorviz.span.landscape.assembler.impl;

import java.io.Serial;
import net.explorviz.span.landscape.assembler.LandscapeAssemblyException;

/**
 * Thrown if a records that is to be inserted into the model is invalid.
 */
public class InvalidRecordException extends LandscapeAssemblyException {
  @Serial
  private static final long serialVersionUID = 1L;

  public InvalidRecordException() {
    super();
  }

  public InvalidRecordException(final String message) {
    super(message);
  }
}
