#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Logger.Serilog"
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote"

open Akka
open System
open System.Threading.Tasks
open Akka.FSharp
open Akka.Actor
open Serilog
open Akka.Pattern

let akkaConfig =
    Configuration.parse("
        akka {
            log-dead-letters = on
            loglevel = DEBUG
        }
    ")


// Initialize random number generator, actor system, and command line parameters
let rnd = System.Random(1)
let system = ActorSystem.Create("GossipProtocol", akkaConfig)
let mutable numNodes = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2] |> string
let algo = fsi.CommandLineArgs.[3] |> string
let gossiprLimit = 10
let gossipsLimit = 1000


// Initialize a stopwatch to measure time
let timer = System.Diagnostics.Stopwatch()

// Define message types for GossipNode actor
type GossipNode =
    | AddTopology of string*IActorRef*list<IActorRef>
    | Rumor
    | Spread
    | Exhausted

// Define message types for Topology actor
type Topology =
    | CreateTopology of string*list<IActorRef>
    | TopologyDone

// Define message types for Gossip simulation
type GossipSimulator =
    | InitialiseTopology of string
    | TopoDone
    | NodeExhausted


// Define the Topology actor
let Topology(mail: Actor<_>) =
    let mutable records = 0
    let mutable topologyref = null
    let rec loop() = actor {
        let! message = mail.Receive()
        match message with
        | CreateTopology(topology, nodeList) ->    
            let mutable nlist = []
            topologyref <- mail.Sender()
            if topology = "line" then
                for i in 0 .. numNodes - 1 do
                    nlist <- []
                    if i <> 0 then
                        nlist <- nlist @ [nodeList.Item(i - 1)]
                    if i <> numNodes - 1 then
                        nlist <- nlist @ [nodeList.Item(i + 1)]
                    if algo = "gossip" then
                        nodeList.Item(i) <! AddTopology(topology, topologyref, nlist)
            elif topology = "full" then
                for i in 0 .. numNodes - 1 do
                    if algo = "gossip" then
                        nodeList.Item(i) <! AddTopology(topology, topologyref, [])
            elif topology = "2D" || topology = "imp2D" then
                let n = sqrt(numNodes |> float) |> int
                numNodes <- n * n
                for i in 0 .. numNodes - 1 do
                    nlist <- []
                    if i % n <> 0 then
                        nlist <- nlist @ [nodeList.Item(i - 1)]
                    if i % n <> n - 1 then
                        nlist <- nlist @ [nodeList.Item(i + 1)]
                    if i / n <> 0 then 
                        nlist <- nlist @ [nodeList.Item(i - n)]
                    if i / n <> n - 1 then 
                        nlist <- nlist @ [nodeList.Item(i + n)]
                    if topology = "imp2D" then 
                        nlist <- nlist @ [nodeList.Item(rnd.Next() % numNodes)]
                    if algo = "gossip" then
                        nodeList.Item(i) <! AddTopology(topology, topologyref, nlist)
        | TopologyDone ->  
            if records = numNodes - 1 then
                topologyref <! TopoDone
            records <- records + 1
        return! loop()
    }
    loop()

// Spawn the Topology actor
let topologyRef = spawn system "topology" Topology

// Initialize a mutable list to hold GossipNode actors
let mutable nodeList = []

// Define the GossipNode actor
let Node(mail: Actor<_>) =
    let mutable neigbhours = []
    let mutable rumorHeard = 0
    let mutable hadRumor = false
    let mutable spreadCount = 0
    let mutable nTopology = ""
    let mutable exhausted = false
    let mutable nodesExhausted = 0
    let mutable simulatorref = null
    let id = mail.Self.Path.Name |> int
    let rec loop() = actor {
        let! message = mail.Receive()

        match message with
        | AddTopology(topology, dref, nodelist) ->
            neigbhours <- nodelist
            nTopology <- topology
            simulatorref <- dref
            mail.Sender() <! TopoDone
        | Rumor ->  
            if not exhausted then
                if rumorHeard = 0 then
                    hadRumor <- true
                rumorHeard <- rumorHeard + 1
                if rumorHeard = gossiprLimit then 
                    exhausted <- true
                    simulatorref <! NodeExhausted
                    if topology = "full" then
                        for i in 0 .. numNodes - 1 do 
                            if i <> id then
                                nodeList.Item(i) <! Exhausted
                    else
                        for i in 0 .. neigbhours.Length - 1 do
                            neigbhours.Item(i) <! Exhausted
                else
                    mail.Self <! Spread
                        
        | Spread -> 
            if not exhausted then
                let mutable next = rnd.Next()
                if topology = "full" then
                    while next % numNodes = id do
                            next <- rnd.Next()
                    nodeList.Item(next % numNodes) <! Rumor
                else
                    neigbhours.Item(next % neigbhours.Length) <! Rumor
                spreadCount <- spreadCount + 1
                if spreadCount = gossipsLimit then
                    exhausted <- true
                    simulatorref <! NodeExhausted
                    if topology = "full" then
                        for i in 0 .. numNodes - 1 do 
                            if i <> id then
                                nodeList.Item(i) <! Exhausted
                    else
                        for i in 0 .. neigbhours.Length - 1 do
                            nodeList.Item(i) <! Exhausted
                else
                    mail.Self <! Spread

        | Exhausted ->  
            if not hadRumor then
                mail.Self <! Rumor
            if not exhausted then
                nodesExhausted <- nodesExhausted + 1
                if topology = "full" then 
                    if nodesExhausted = numNodes - 1 then 
                        exhausted <- true
                        simulatorref <! NodeExhausted
                else
                    if nodesExhausted = neigbhours.Length then
                        exhausted <- true
                        simulatorref <! NodeExhausted
        return! loop()
    }
    loop()

// Spawn GossipNode actors based on the chosen algorithm
if algo = "gossip" then
    nodeList <- [for a in 0 .. numNodes - 1 do yield(spawn system (string a) Node)] 
// TODO: Add Push-Sum part

// Define the Gossip Simulator actor
let GossipSimulator(mail: Actor<_>) =
    let mutable spread = 0  
    let rec loop() = actor {
        let! message = mail.Receive()
        match message with
        | InitialiseTopology(topology) ->     
            topologyRef <! CreateTopology(topology, nodeList)
        | TopoDone ->  
            if algo = "gossip" then
                nodeList.Item(rnd.Next() % numNodes) <! Rumor
                timer.Start()
            // TODO: Add Push-Sum Part

        | NodeExhausted ->  
            spread <- spread + 1
            if spread = numNodes then 
                mail.Context.System.Terminate() |> ignore
                printfn "%s,%s,%i,%i" algo topology numNodes timer.ElapsedMilliseconds
        return! loop()
    }
    loop()

// Spawn the Dispatcher actor
let simulatorRef = spawn system "GossipSimulator" GossipSimulator
  


// code to check if gossip node actors are created!
// let actorPath = "/user/GossipNode"  // Path to the actor
// let actorRef = system.ActorSelection(actorPath)

// let actorExistsTask =
//     actorRef.ResolveOne(TimeSpan.FromSeconds(5.0))

// let actorExists =
//     async {
//         let! result = actorExistsTask |> Async.AwaitTask
//         return not (result = null)
//     }
//     |> Async.RunSynchronously

// if actorExists then
//     printfn "The actor is registered."
// else
//     printfn "The actor is not registered."

// let nodeIdToCheck = 2 // Change this to the desired node ID
// let actorP = sprintf "akka://GossipProtocol/user/GossipNode%d" nodeIdToCheck
// let actorR =
//     try
//         let task = system.ActorSelection(actorP).ResolveOne(TimeSpan.FromSeconds(1.0))
//         Async.AwaitIAsyncResult(task) |> ignore // Ignore the result; we're only interested in exceptions
//         printfn "Gossip node %d is registered." nodeIdToCheck
//     with
//     | :? Akka.Actor.ActorNotFoundException ->
//         printfn "Gossip node %d is not registered." nodeIdToCheck
//     | ex ->
//         printfn "An error occurred: %s" ex.Message