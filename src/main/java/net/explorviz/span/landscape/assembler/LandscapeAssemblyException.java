package net.explorviz.span.landscape.assembler;

import java.io.Serial;

/**
 * Thrown if a landscape could not be assembled.
 */
public class LandscapeAssemblyException extends RuntimeException {
  @Serial
  private static final long serialVersionUID = -5122287416640668604L;

  public LandscapeAssemblyException() {
    super();
  }

  public LandscapeAssemblyException(final String message) {
    super(message);
  }

  public LandscapeAssemblyException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public LandscapeAssemblyException(final Throwable cause) {
    super(cause);
  }

  public LandscapeAssemblyException(final String message, final Throwable cause,
      final boolean enableSuppression, final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
