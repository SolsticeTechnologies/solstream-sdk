/**
 * Typed error classes mirroring the Rust SDK's ClientError / DecodeError.
 */

export type ClientErrorKind =
  | 'transport'      // gRPC channel failed
  | 'invalidEndpoint'
  | 'invalidApiKey'
  | 'status'         // server returned a gRPC error status
  | 'decode'         // proto → type conversion failed
  | 'cancelled';     // caller called cancel()

export class ClientError extends Error {
  readonly kind: ClientErrorKind;
  readonly cause?: Error;

  constructor(kind: ClientErrorKind, message: string, cause?: Error) {
    super(message);
    this.name = 'ClientError';
    this.kind = kind;
    this.cause = cause;
  }

  static transport(cause: Error): ClientError {
    return new ClientError('transport', `transport error: ${cause.message}`, cause);
  }

  static invalidEndpoint(msg: string): ClientError {
    return new ClientError('invalidEndpoint', `invalid endpoint: ${msg}`);
  }

  static invalidApiKey(): ClientError {
    return new ClientError('invalidApiKey', 'API key must be ASCII');
  }

  static status(code: number, msg: string): ClientError {
    return new ClientError('status', `gRPC status ${code}: ${msg}`);
  }

  static decode(msg: string): ClientError {
    return new ClientError('decode', `decode error: ${msg}`);
  }

  static cancelled(): ClientError {
    return new ClientError('cancelled', 'stream cancelled');
  }
}
