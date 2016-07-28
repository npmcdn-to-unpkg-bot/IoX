open System.IO
open System.IO.Compression
open System.Net

let safeRead len (stream:Stream) =
  let data = Array.zeroCreate len
  let mutable offset = 0
  let mutable count = len
  while count <> 0 do
    let read = stream.Read(data, offset, count)
    if read = 0 then
      failwith "EOS"
    else
      offset <- offset + read
      count <- count - read
  data

let safeReadLen (stream:Stream) =
  try
    safeRead 4 stream
    |> Array.fold (fun s v -> (s <<< 8) + int v) 0
    |> Some
  with _ -> None

[<EntryPoint>]
let main (args:string[]) =
  let hostname, port =
    match args.[0].Split(':') with
    | [|host; port|] -> host, System.Int32.Parse(port)
    | _ -> failwith "Invalid syntax, the first argument should be the destination host:port"
  use socket = new Sockets.UdpClient()
  use stream = System.Console.OpenStandardInput()
  let rec sendMsg () =
    match safeReadLen stream with
    | None -> ()
    | Some len ->
      let msg = safeRead len stream
      try printfn "%A %s" msg.Length (System.Text.Encoding.UTF8.GetString(msg)) with _ -> ()
      socket.Send(msg, len, hostname, port) |> ignore
      sendMsg ()
  sendMsg()
  0
