declare module "*/wa-sqlite/wa-sqlite-async.mjs" {
  const moduleFactory: (config?: { locateFile?: (path: string) => string }) => Promise<any>;
  export default moduleFactory;
}

declare module "*/wa-sqlite/sqlite-api.js" {
  export function Factory(module: any): any;
}
