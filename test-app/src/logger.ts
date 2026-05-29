/**
 * Minimal coloured terminal logger â€” no extra dependencies.
 * Uses ANSI escape codes directly.
 */

const C = {
  reset:   '\x1b[0m',
  bold:    '\x1b[1m',
  dim:     '\x1b[2m',
  cyan:    '\x1b[36m',
  green:   '\x1b[32m',
  yellow:  '\x1b[33m',
  blue:    '\x1b[34m',
  magenta: '\x1b[35m',
  red:     '\x1b[31m',
  white:   '\x1b[37m',
};

function ts(): string {
  return new Date().toISOString().replace('T', ' ').replace('Z', '');
}

export function banner(title: string): void {
  const line = 'â”€'.repeat(60);
  console.log(`\n${C.cyan}${C.bold}${line}${C.reset}`);
  console.log(`${C.cyan}${C.bold}  ${title}${C.reset}`);
  console.log(`${C.cyan}${C.bold}${line}${C.reset}\n`);
}

export function info(label: string, ...args: unknown[]): void {
  console.log(`${C.dim}${ts()}${C.reset}  ${C.blue}${C.bold}[${label}]${C.reset}`, ...args);
}

export function success(label: string, ...args: unknown[]): void {
  console.log(`${C.dim}${ts()}${C.reset}  ${C.green}${C.bold}[${label}]${C.reset}`, ...args);
}

export function warn(label: string, ...args: unknown[]): void {
  console.log(`${C.dim}${ts()}${C.reset}  ${C.yellow}${C.bold}[${label}]${C.reset}`, ...args);
}

export function error(label: string, ...args: unknown[]): void {
  console.error(`${C.dim}${ts()}${C.reset}  ${C.red}${C.bold}[${label}]${C.reset}`, ...args);
}

export function stat(label: string, value: unknown, unit = ''): void {
  console.log(
    `${C.dim}${ts()}${C.reset}  ${C.magenta}${C.bold}[STAT]${C.reset}  ` +
    `${C.white}${label}:${C.reset} ${C.bold}${value}${C.reset}${unit ? ` ${C.dim}${unit}${C.reset}` : ''}`,
  );
}

export function separator(): void {
  console.log(`${C.dim}${'Â·'.repeat(60)}${C.reset}`);
}

/** Truncate a base58/base64 key for display */
export function short(val: Uint8Array | string | undefined, len = 8): string {
  if (!val) return '<null>';
  const s = typeof val === 'string' ? val : Buffer.from(val).toString('base58' as BufferEncoding);
  return `${s.slice(0, len)}â€¦`;
}

/** Format bigint lamports as SOL */
export function lamportsToSol(lamports: bigint | number | undefined): string {
  if (lamports === undefined) return '?';
  return (Number(lamports) / 1e9).toFixed(4) + ' SOL';
}

