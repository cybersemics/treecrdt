export type RibltCodeword16 = {
  count: number;
  keySum: Uint8Array; // 8 bytes (big-endian u64)
  valueSum: Uint8Array; // 16 bytes (symbol XOR)
};
