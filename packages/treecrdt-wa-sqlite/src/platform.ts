export function isNode(): boolean {
  return typeof process !== 'undefined' && typeof process.versions?.node === 'string';
}

export function isBrowser(): boolean {
  return !isNode();
}
