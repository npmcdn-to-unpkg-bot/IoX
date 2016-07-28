namespace IoX.Module.Syslog

open IoX.Modules
open EvReact.Expr
open Suave
open Suave.Operators
open Suave.EvReact
open System.Net
open System.IO
open System.IO.Compression
open System

type CollectorConfiguration = {
  Destination: Uri
  ForwardPriorityThreshold: int
  DumpPriorityThreshold: int
  SyslogPort: int
  BufferSizeThreshold: int
  BufferTimeoutMS: int
  Verbose: bool
}

type DispatcherConfiguration = {
  DestinationHost: string
  DestinationPort: int
  Verbose: bool
}

type CollectorStats = {
  ElapsedMS: int64
  RawNetBytes: int64
  CompressedNetBytes: int64
  RawDiskBytes: int64
  CompressedDiskBytes: int64
}

type DispatcherStats = {
  ElapsedMS: int64
  RawNetBytes: int64
  CompressedNetBytes: int64
}

type ParsedMessage = {
  Priority: int
  Data: byte[]
}

[<Module(
  Name = "Syslog collector",
  Description = "IoX module that collects, compresses and forwards syslog messages to an IoX syslog dispatcher."
)>]
type CollectorModule(data: IModuleData<CollectorConfiguration>) as this =
  inherit DriverModule()

  let isDigit b = byte '0' <= b && b <= byte '9'
  let readDigit b = int b - int '0'
  let parsePriority (msg:byte[]) =
    if msg.[0] <> byte '<' then
      failwith "Syslog message does not begin with '<'"
    let mutable r = 0
    let mutable idx = 1
    while idx <= 3 && isDigit msg.[idx] do
      r <- 10*r + readDigit msg.[idx]
      idx <- idx + 1
    if idx = 1 then
      failwith "Syslog message does not have a priority number"
    elif msg.[idx] <> byte '>' then
      failwith "Syslog message priority is not followed by '>'"
    else
      r

  let orch = EvReact.Orchestrator.create()
  let atomicAction f =
    let evt = EvReact.Event()
    EvReact.Utils.start0 orch +(!!evt.Publish |-> f) |> ignore
    fun _ -> evt.Trigger(Unchecked.defaultof<_>)

  let mutable fileStream = null
  let mutable dumpStream : GZipStream = null
  let renewDumpStream _ =
    if not(isNull dumpStream) then
      dumpStream.Close()
      dumpStream.Dispose()
    // Some filesystems prohibit ':' in filenames
    let timeStamp = DateTime.UtcNow.ToString("o").Replace(":", "")
    fileStream <- File.Create(Path.Combine(data.Path, "log", sprintf "syslog.%s.dat.gz" timeStamp))
    dumpStream <- new GZipStream(fileStream, CompressionLevel.Optimal)
  let renewDumpStream = atomicAction renewDumpStream

  let buffer = ResizeArray<_>()
  let mutable count = 0

  let stopwatch = System.Diagnostics.Stopwatch()
  let mutable rawNetBytes = 0L
  let mutable compressedNetBytes = 0L
  let mutable rawDiskBytes = 0L

  let sendJson json =
    use compressed = new MemoryStream()
    use stream = new GZipStream(compressed, CompressionLevel.Optimal)
    stream.Write(json, 0, json.Length)
    stream.Close()
    let compressedJson = compressed.ToArray()
    if data.Configuration.Data.Verbose then
      printfn "Compressed %d bytes of JSON to %d bytes" json.Length compressedJson.Length
    compressedNetBytes <- compressedNetBytes + compressedJson.LongLength
    use client = new System.Net.WebClient()
    client.Headers.[HttpRequestHeader.ContentType] <- "application/json"
    client.Headers.[HttpRequestHeader.ContentEncoding] <- "gzip"
    client.UploadData(data.Configuration.Data.Destination, compressedJson) |> ignore

  let sendMessages tunMsg =
    async {
      try createRemoteTrigger sendJson tunMsg
      with e -> printfn "Could not send message: %A" e
    } |> Async.Start

  let forwardMessage msg =
    sendMessages [| msg.Data |]
    if data.Configuration.Data.Verbose then
      printfn "Message of %A bytes with priority %A (severity %A) forwarded" msg.Data.Length msg.Priority (msg.Priority%8)
    rawNetBytes <- rawNetBytes + msg.Data.LongLength

  let dumpMessage msg =
    let len = msg.Data.Length
    dumpStream.WriteByte(byte (len >>> 24))
    dumpStream.WriteByte(byte (len >>> 16))
    dumpStream.WriteByte(byte (len >>> 8))
    dumpStream.WriteByte(byte (len >>> 0))
    dumpStream.Write(msg.Data, 0, len)
    dumpStream.Flush()
    rawDiskBytes <- rawDiskBytes + msg.Data.LongLength
    printfn "Message of %A bytes with priority %A (severity %A) stored on disk" len msg.Priority (msg.Priority%8)

  let newMessage = EvReact.Event.create "newMessage"
  let onNewMessage = newMessage.Publish
  let flushBuffer _ =
    let messages = buffer.ToArray()
    rawNetBytes <- Array.fold (fun s (msg:_[]) -> s + msg.LongLength) rawNetBytes messages
    sendMessages messages
    count <- 0
    buffer.Clear()
  let flushBuffer = atomicAction flushBuffer

  let dumpTimer = new Threading.Timer(Threading.TimerCallback(renewDumpStream))
  let bufferTimer = new Threading.Timer(Threading.TimerCallback(flushBuffer))

  let bufferMessage msg =
    if data.Configuration.Data.Verbose then
      printfn "Message of %A bytes with priority %A (severity %A) buffered" msg.Data.Length msg.Priority (msg.Priority%8)
    buffer.Add(msg.Data)
    count <- count + msg.Data.Length
    if count > data.Configuration.Data.BufferSizeThreshold then
      flushBuffer ()
    elif buffer.Count = 1 then
      if data.Configuration.Data.Verbose then printfn "Starting timer"
      bufferTimer.Change(
        dueTime = data.Configuration.Data.BufferTimeoutMS,
        period  = Threading.Timeout.Infinite)
      |> ignore

  let isHighPriority msg = msg.Priority % 8 <= data.Configuration.Data.ForwardPriorityThreshold
  let isLowPriority msg = msg.Priority % 8 >= data.Configuration.Data.DumpPriorityThreshold
  let isNormalPriority msg = not (isLowPriority msg || isHighPriority msg)

  let highPriority   = +(onNewMessage %- isHighPriority |-> forwardMessage)
  let normalPriority = +(onNewMessage %- isNormalPriority |-> bufferMessage)
  let dumpToDisk     = +(!!onNewMessage |-> dumpMessage)

  let mutable socket = null
  let mutable needSocketRefresh = true

  let refreshSocket() =
    needSocketRefresh <- false
    if not(isNull socket) then
      (socket :> IDisposable).Dispose()
    socket <- new Sockets.UdpClient(data.Configuration.Data.SyslogPort)
    if data.Configuration.Data.Verbose then
      printfn "Waiting for datagrams on port %A" data.Configuration.Data.SyslogPort

  let rec beginReceive() =
    socket.BeginReceive(AsyncCallback cb, socket) |> ignore
  and cb result =
    try
      let mutable endPoint = IPEndPoint(IPAddress.Any, 0)
      let recvSocket = result.AsyncState :?> Sockets.UdpClient
      let msg = recvSocket.EndReceive(result, &endPoint)
      if needSocketRefresh then
        // The socket is about to change, hence the data received on the current
        // port is going to be ignored in any case.
        ()
      else
        if data.Configuration.Data.Verbose then
          try printfn "%A %s" msg.Length (System.Text.Encoding.UTF8.GetString(msg)) with _ -> ()
        let args = {
          Data = msg
          Priority = parsePriority msg
        }
        async { newMessage.Trigger(args) } |> Async.Start
    with e -> printfn "Unexpected error: %A" e
    if needSocketRefresh then
      refreshSocket()
    beginReceive()

  let updateConfig (_,cfg) =
    let oldPort = data.Configuration.Data.SyslogPort
    data.Configuration.Data <- cfg
    data.Configuration.Save()
    if data.Configuration.Data.Verbose then
      printfn "Configuration updated"
    if oldPort <> cfg.SyslogPort then
      // Send an illegal message to get to the callback
      needSocketRefresh <- true
      socket.SendAsync([||], 0, IPEndPoint(IPAddress.Loopback, oldPort)) |> ignore

  let getConfig (ctx:MsgRequestEventArgs<_>) =
    ctx.Result <- this.BuildJsonReply data.Configuration.Data

  let collectStats (ctx:MsgRequestEventArgs<_>) =
    ctx.Result <- this.BuildJsonReply {
      ElapsedMS           = stopwatch.ElapsedMilliseconds
      RawNetBytes         = rawNetBytes
      CompressedNetBytes  = compressedNetBytes
      RawDiskBytes        = rawDiskBytes
      CompressedDiskBytes = fileStream.Position
    }

  do
    stopwatch.Start()
    this.Root <- Redirection.moved_permanently "index.html"
    this.Browsable <- true

    +(!!this.RegisterReplyEvent("stats") |-> collectStats)
    |> this.ActivateNet
    |> ignore

    +(!!this.RegisterReplyEvent("getConfig") |-> getConfig)
    |> this.ActivateNet
    |> ignore

    +(!!this.RegisterEvent("saveConfig") |-> updateConfig )
    |> this.ActivateNet
    |> ignore

    +(!!this.RegisterEvent("reloadConfig") |-> (fun _ -> data.Configuration.Load()))
    |> this.ActivateNet
    |> ignore

    EvReact.Utils.start0 orch highPriority   |> ignore
    EvReact.Utils.start0 orch dumpToDisk     |> ignore
    EvReact.Utils.start0 orch normalPriority |> ignore
    let msInADay = 24 * 60 * 60 * 1000
    let currMS = int System.DateTime.Now.TimeOfDay.TotalMilliseconds
    dumpTimer.Change(dueTime=msInADay-currMS, period=msInADay) |> ignore
    renewDumpStream()
    refreshSocket()
    beginReceive()

  static member DefaultConfig = {
    Destination = Uri("http://192.0.2.2:8080/tunnel")
    ForwardPriorityThreshold = 2
    DumpPriorityThreshold = 5
    SyslogPort = 514
    BufferSizeThreshold = 1024*1024
    BufferTimeoutMS = 10 * 1000
    Verbose = false
  }

[<Module(
  Name = "Syslog dispatcher",
  Description = "IoX module that decompresses and forwards messages from an IoX syslog collector to a syslog server."
)>]
type DispatcherModule(data: IModuleData<DispatcherConfiguration>) as this =
  inherit DriverModule()

  let stopwatch = System.Diagnostics.Stopwatch()
  let mutable rawNetBytes = 0L
  let mutable compressedNetBytes = 0L
  let socket = new Sockets.UdpClient()

  let sendMessage msg =
    socket.Send(
      msg,
      msg.Length,
      data.Configuration.Data.DestinationHost,
      data.Configuration.Data.DestinationPort)
    |> ignore
    rawNetBytes <- rawNetBytes + msg.LongLength
    if data.Configuration.Data.Verbose then
      try printfn "%A %s" msg.Length (System.Text.Encoding.UTF8.GetString(msg)) with _ -> ()

  let sendData (ctx:HttpEventArgs) =
    ctx.Result <- Successful.OK ""
    if data.Configuration.Data.Verbose then
      printfn "Got %A bytes" ctx.Context.request.rawForm.Length
    compressedNetBytes <- compressedNetBytes + ctx.Context.request.rawForm.LongLength
    use compressed = new MemoryStream(ctx.Context.request.rawForm)
    use stream = new GZipStream(compressed, CompressionMode.Decompress)
    use reader = new StreamReader(stream, Text.Encoding.UTF8)
    let json = reader.ReadToEnd()
    let messages = Newtonsoft.Json.JsonConvert.DeserializeObject<_>(json)
    for m in messages do
      sendMessage m

  let updateConfig (_,cfg) =
    data.Configuration.Data <- cfg
    data.Configuration.Save()
    if data.Configuration.Data.Verbose then
      printfn "Configuration updated"

  let getConfig (ctx:MsgRequestEventArgs<_>) =
    ctx.Result <- this.BuildJsonReply data.Configuration.Data

  let collectStats (ctx:MsgRequestEventArgs<_>) =
    ctx.Result <- this.BuildJsonReply {
      DispatcherStats.ElapsedMS = stopwatch.ElapsedMilliseconds
      RawNetBytes               = rawNetBytes
      CompressedNetBytes        = compressedNetBytes
    }

  do
    stopwatch.Start()
    this.Root <- Redirection.moved_permanently "index.html"
    this.Browsable <- true

    +(!!this.RegisterHttpEvent("tunnel") |-> sendData)
    |> this.ActivateNet
    |> ignore

    +(!!this.RegisterReplyEvent("stats") |-> collectStats)
    |> this.ActivateNet
    |> ignore

    +(!!this.RegisterReplyEvent("getConfig") |-> getConfig)
    |> this.ActivateNet
    |> ignore

    +(!!this.RegisterEvent("saveConfig") |-> updateConfig)
    |> this.ActivateNet
    |> ignore

    +(!!this.RegisterEvent("reloadConfig") |-> fun _ -> data.Configuration.Load())
    |> this.ActivateNet
    |> ignore

  static member DefaultConfig = {
    DestinationHost = "192.0.2.3" // example destination
    DestinationPort = 514
    Verbose = false
  }
