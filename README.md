# CoflowSim README

**CoflowSim** is a flow-level simulator to compare various coflow scheduling heurisitcs against traditional per-flow scheduling techniques. 

The entire project (**CoflowSim** and the actual implementation **Varys**) is still in closed beta stage. The master branch is in version 0.2.0-SNAPSHOT.

## How to Compile
**CoflowSim** does not have any dependency on external library.

* You can compile from command line using Maven. Simply type `mvn package` from the root directory.
* Import the project into Eclipse also works.

## How to Run
The main method of **CoflowSim** is in the `coflowsim.CoflowSim` class, which takes and various inputs, creates appropriate scheduler, and performs the simulation.

Using the `exec-maven` plugin:

```
mvn exec:java -Dexec.mainClass="coflowsim.CoflowSim" -Dexec.args="<arguments>"
```

From the command-line (assuming the jar to be in the `target` directory):

```
java -cp target/coflowsim-*.jar coflowsim.CoflowSim <arguments>
```

## Supported Modes and Scheduling Algorithms
The simulator can work in two modes, i.e., to optimize to objectives.

1. Minimize coflow completion time (CCT)
2. Meet deadline

It has multiple heuristics and algorithms implemented already. 
For clairvoyant coflow-based scheduling **CoflowSim** supports the following heuristics:

1. **FIFO**: First-In-First-Out at the coflow level
2. **SCF**: Shortest-Coflow-First
3. **NCF**: Narrowest-Coflow First
4. **LCF**: Lightest-Coflow-First 
5. **SEBF**: Smallest-Effective-Bottleneck-First / Smallest-Skew-First

**CoflowSim** also has a non-clairvoyant coflow-based scheduler that divides coflows into multiple logical queues based on how much they have already sent:

1. **DARK**: FIFO within the queue, and for now, strict prioriries across queues.

For per-flow scheduling, **CoflowSim** supports:

1. **FAIR**: Per-flow fair sharing
2. **PFP**: Per-flow prioritization, i.e., SRTF for minimizing time and EDF for meeting deadlines. Examples include PDQ and pFabric.

## Workloads
**CoflowSim** currently provides two `TraceProducer` classes to generate/use workloads. 

1. **CUSTOM** (`CustomTraceProducer`) creates synthetic workload based on user-defined parameters.
2. **COFLOW-BENCHMARK** (`CoflowBenchmarkTraceProducer`) reads workloads based on traces from the filesystem. These workloads are part of the <a href="https://github.com/coflow/coflow-benchmark">Coflow-Benchmark</a> project that focuses on synthesizing coflow traces from real-world workloads.

## Documentation
You can take a quick look at the `coflowsim.CoflowSim` class to know more about how to pass the aforementioned parameters to it.

The code itself is fairly documented with `javadoc`, and you can run the `mvn javadoc:javadoc` to generate the documentation.

## Contribute
Please submit an <a href="https://github.com/coflow/coflowsim/issues">issue</a> or a <a href="https://github.com/coflow/coflowsim/pulls">pull request</a> to help us keep **CoflowSim** up-to-date. 
If you are using Eclipse, run your code through this project's <a href="https://github.com/coflow/coflowsim/blob/master/CoflowSim-EclipseFormatter.xml">code formatter</a> before sending the pull request.

## References
Please refer to/cite the following papers based on the scheduler you are using: the former for **SEBF** and the latter for **DARK**.

1. <a href="http://www.mosharaf.com/wp-content/uploads/varys-sigcomm14.pdf">Efficient Coflow Scheduling with Varys</a>, Mosharaf Chowdhury, Yuan Zhong, Ion Stoica, ACM SIGCOMM, 2014.
2. <a href="http://www.mosharaf.com/wp-content/uploads/aalo-sigcomm15.pdf">Efficient Coflow Scheduling Without Prior Knowledge</a>, Mosharaf Chowdhury, Ion Stoica, ACM SIGCOMM, 2015.
