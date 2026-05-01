declare module '*?sharedworker' {
  const WorkerWrapper: (options?: WorkerOptions & { name?: string }) => SharedWorker;
  export default WorkerWrapper;
}
