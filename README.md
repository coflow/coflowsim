#CoflowSim README

**CoflowSim** is a flow-level simulator to compare various coflow scheduling heurisitcs against traditional per-flow scheduling techniques. 

The entire project (**CoflowSim** and the actual implementation **Varys**) is still in closed beta stage. The master branch is in version 0.0.1-SNAPSHOT.

##How to Compile
**CoflowSim** does not have any dependency on external library.

* You can compile from command line using Maven. Simply type `mvn package` from the root directory.
* Import the project into **Eclipse** also works.
* Finally, `mvn javadoc:javadoc`will build the documentation.

##How to Run
The main method of **CoflowSim** is in the `CoflowSim` class, which takes and various inputs, creates appropriate scheduler, and performs the simulation.

##Supported Modes and Scheduling Algorithms
The simulator can work in two modes, i.e., to optimize to objectives.

1. Minimize coflow completion time (CCT)
2. Meet deadline

It has multiple heuristics and algorithms implemented already. For coflow-based scheduling **CoflowSim** supports the following heuristics:

1. **FIFO**: First-In-First-Out at the coflow level
2. **SCF**: Shortest-Coflow-First
3. **NCF**: Narrowest-Coflow First
4. **LCF**: Lightest-Coflow-First 
5. **GAMMA**: Smallest-Skew-First

For per-flow scheduling, **CoflowSim** supports:

1. **FAIR**: Per-flow fair sharing
2. **PDQ**: Per-flow prioritization, i.e., SRTF for minimizing time and EDF for meeting deadlines.
