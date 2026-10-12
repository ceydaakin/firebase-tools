import { FirebaseError } from "../../../error";
import { assertExhaustive } from "../../../functional";
import { logger } from "../../../logger";

type TokenFetchState = "NONE" | "FETCHING" | "VALID";
interface TokenFetchResult {
  token?: string;
  aborted: boolean;
}

/**
 * GCF v1 deploys support reusing a build between function deploys.
 * This class will return a resolved promise for its first call to tokenPromise()
 * and then will always return a promise that is resolved by the poller function.
 */
export class SourceTokenScraper {
  private tokenValidDurationMs;
  private resolve!: (token: TokenFetchResult) => void;
  private promise: Promise<TokenFetchResult>;
  private expiry: number | undefined;
  private fetchState: TokenFetchState;
  private currentFetcher: symbol | undefined;

  constructor(validDurationMs = 1500000) {
    this.tokenValidDurationMs = validDurationMs;
    this.promise = new Promise((resolve) => (this.resolve = resolve));
    this.fetchState = "NONE";
  }

  /**
   * Runs an operation with a source token.
   * If the operation throws, automatically aborts the token fetch to prevent deadlocks.
   */
  async withToken<T>(fn: (getToken: () => Promise<string | undefined>) => Promise<T>): Promise<T> {
    const callerId = Symbol();
    const initialToken = await this.getTokenForCaller(callerId);
    let firstAttempt = true;
    try {
      return await fn(async () => {
        if (firstAttempt) {
          firstAttempt = false;
          if (initialToken === undefined) {
            return undefined;
          }
        }
        return this.getTokenForCaller(callerId);
      });
    } catch (err) {
      if (this.currentFetcher === callerId) {
        this.abort();
      }
      throw err;
    }
  }

  private abort(): void {
    if (this.fetchState === "FETCHING") {
      this.fetchState = "NONE";
      this.currentFetcher = undefined;
      const resolve = this.resolve;
      this.promise = new Promise((r) => (this.resolve = r));
      resolve({ aborted: true });
    }
  }

  private async getTokenForCaller(callerId: symbol): Promise<string | undefined> {
    if (this.fetchState === "NONE") {
      this.fetchState = "FETCHING";
      this.currentFetcher = callerId;
      return undefined;
    } else if (this.fetchState === "FETCHING") {
      if (this.currentFetcher === callerId) {
        return undefined;
      }
      const tokenResult = await this.promise;
      if (tokenResult.aborted) {
        return undefined;
      }
      return tokenResult.token;
    } else if (this.fetchState === "VALID") {
      if (this.isTokenExpired()) {
        this.fetchState = "FETCHING";
        this.currentFetcher = callerId;
        this.promise = new Promise((resolve) => (this.resolve = resolve));
        return undefined;
      }
      const tokenResult = await this.promise;
      return tokenResult.token;
    } else {
      assertExhaustive(this.fetchState);
    }
  }

  private isTokenExpired(): boolean {
    if (this.expiry === undefined) {
      throw new FirebaseError(
        "Your deployment is checking the expiration of a source token that has not yet been polled. " +
          "Hitting this case should never happen and should be considered a bug. " +
          "Please file an issue at https://github.com/firebase/firebase-tools/issues " +
          "and try deploying your functions again.",
      );
    }
    return Date.now() >= this.expiry;
  }

  get poller() {
    return (op: any) => {
      if (op.metadata?.sourceToken || op.done) {
        const [, , , /* projects*/ /* project*/ /* regions*/ region] =
          op.metadata?.target?.split("/") || [];
        logger.debug(`Got source token ${op.metadata?.sourceToken} for region ${region as string}`);
        this.resolve({
          token: op.metadata?.sourceToken,
          aborted: false,
        });
        this.fetchState = "VALID";
        this.currentFetcher = undefined;
        this.expiry = Date.now() + this.tokenValidDurationMs;
      }
    };
  }
}
