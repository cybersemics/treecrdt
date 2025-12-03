# em Proposal Beta

## Background *Assume everyone is up to date why we are building this*

## Scopes

We propose dividing the work into two scopes. 

- The Sync Engine scope “Sync engine”   
- EM data and identity implementation scope “EM DataProvider”. 

## Sync engine

### Goal 

Provide a library that allows developers to set up a Tree CRDT and related functionality to handle synchronization and authentication. 

**Tree CRDT**

- An implementation of the Kleppman Tree CRDT

**Synchronization**

- Allow any two nodes to reconcile  
- Conditional synchronization (partial synchronization “only sync children of node X”) 

**Access control** 

- Allow for read and write access control of trees and subtrees

**Capabilities**

- Can run in browser and natively

**Requirements**

Satisfy   
[Requirements](https://github.com/cybersemics/em/wiki/Local%E2%80%90First-Sync-Engine#requirements)

	

## Solution

### Background

The previous [proposal](https://github.com/cybersemics/partykit-em/blob/main/PROPOSAL.md) proposes the idea of using the Kleppman Tree CRDT as a go to CRDT solution for EM. We agree this is a good idea but the implementation proposal should be refined. We propose that instead of writing the CRDT in SQL code a better approach would be to build the CRDT in a programming language and then embed it in a SQLite extension.  

Here are the arguments: 

- With SQL code there is a risk of hitting the limits optimization one can apply. One e.g. how can we optimize the recursion step in the insertion of nodes? In a programming language we could potentially make the loop-check be smarter by re-using loop checks. This kind of optimization is hard or impossible to do in SQL code.   
- Even though the reference implementation is quite performant. [https://github.com/cybersemics/partykit-em/tree/main/src](https://github.com/cybersemics/partykit-em/tree/main/src)

  Benchmark name | time (ms)   
  local-mem-insert-chain-length-100 | 184.94 | | local-mem-insert-chain-length-1000 | 1537.93 | | local-mem-insert-chain-length-10000 | 17150.12 | | local-mem-bulk-insert-root-siblings-batch-100 | 13.49 | | local-mem-bulk-insert-root-siblings-batch-1000 | 61.51 | | local-mem-bulk-insert-root-siblings-batch-10000 | 485.75 |

  When we access control, networking, and other potential features, and additionally run EM on low-performant devices, we need a great margin towards the performance requirement if we only consider the TreeCRDT implementation  
    
- DX experience. It is very hard to debug SQL(ite) in comparison to a programming language solution where you can natively debug and test.  
- For future development, a programming language solution opens more doors if and so a SQLIte extension becomes unwanted and we for example want to write a custom storage and indexing solution to server the operation log and the node tree  
- Community usability. With a programming language solution at the lowest level, developers can enjoy a highly performant TreeCRDT implementation without any assumptions about how it is used, instead the storage, indexing is abstracted away.   
- Migrations and versioning, we have more control on how to handle migrations and reduce associated risks.  
- We need to be able to support the use of the TreeCRDT potentially with other CRDTs like a Fractional Index CRDT to allow peers to organize siblings. This implementation will most likely add an additional complexity layer which speaks for a solution where we have more control over the code.   
    

**Implementation**  
	

- Create a Typescript interface for the TreeCRDT  
- Implement the TreeCRDT as Rust library   
- Write a SQLite extension that uses the TreeCRDT library. This SQLite extension allows the TreeCRDT to be persistent and allows for performant fetches of operations through the indexes that can be built. Implement the Typescript TreeCRDT interface for the SQLite extension.   
- Create a synchronization library in Typescript that depends on the TreeCRDT interface and a networking stack. This library can make any two nodes reconcile.  
  

  
The miro board describes the implementation in more details  
[https://miro.com/app/board/uXjVJjjroC0=/?share\_link\_id=737124298913](https://miro.com/app/board/uXjVJjjroC0=/?share_link_id=737124298913)

**Notes**  
[cr-sqlite](https://github.com/vlcn-io/cr-sqlite) uses Rust for CRDT implementation.It doesn’t support WASM compilation and can’t be used in a local-first context.

For WASM support it has separate project : [https://github.com/vlcn-io/js](https://github.com/vlcn-io/js)  
JS implementation of cr-sqlite uses Rust implementation of CRDT and communication protocol and links it with wa-sqlite. 

This project proves that it’s possible to implement Rust CRDT with SQL extension integration. Some extra work for infrastructure is needed in order to compile all the libraries together.


### Planning

Coming 2 weeks 

- Setup the 2 repos  
- Start to work on the TreeCRDT and the abstractions.

  Note: [https://github.com/cybersemics/partykit-em/blob/main/PROPOSAL.md](https://github.com/cybersemics/partykit-em/blob/main/PROPOSAL.md) proposes an extension of the TreeCRDT to provide an alternative tombstone behaviour. Analyze this.

- Define the interface in Typescript for the TreeCRDT.  
- Add TreeCRDT unit tests which cover corner cases of different moves/deletion/inserts etc.  
- Define synchronization protocol and necessary metadata needed for this  
  - If time, research how we can show synchronization progress
