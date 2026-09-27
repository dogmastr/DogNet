# DogNet

**The fastest networking library on Roblox: on average 9x faster than RemoteEvents, with 8.6x less bandwidth.**

DogNet replaces the RemoteEvents and RemoteFunctions in your game. You list the messages your game sends and what's in them, and DogNet packs everything sent in a frame into one small binary message, so the game spends less time sending and receiving, and players download less.

Similar to ByteNet Max, DogNet uses the same API: `defineNamespace`, `definePacket`, `defineQuery` and the same data types.

## Contents

- [Benchmarks](#benchmarks)
- [Why use it](#why-use-it)
- [Install](#install)
- [Quick start](#quick-start)
- [The four ideas](#the-four-ideas)
- [Packets: sending messages](#packets-sending-messages)
- [Queries: asking for an answer](#queries-asking-for-an-answer)
- [Data types](#data-types)
- [Security: never trust the client](#security-never-trust-the-client)
- [Settings](#settings)
- [Troubleshooting](#troubleshooting)
- [How it works](#how-it-works)
- [Coming from ByteNet Max](#coming-from-bytenet-max)

## Benchmarks

**Speed:** client FPS while sending 1,000 messages every frame. Higher is better.

| Test | RemoteEvent | ByteNet Max | Blink | QuickNet | DogNet |
|---|---:|---:|---:|---:|---:|
| 1,000 booleans | 4.9 | 20.8 | 68.5 | 134.6 | **156.2** (32x) |
| 100 entities | 4.9 | 24.3 | 31.2 | 42.7 | **56.2** (11x) |
| 500 strings, all the same | 6.5 | 0.9 | 29.2 | 42.5 | **86.2** (13x) |
| 500 strings, all different | 6.7 | 0.8 | – | 31.8 | **32.5** (4.9x) |
| 2,000 numbers | 5.2 | 7.5 | 23.0 | 31.3 | **32.5** (6.3x) |
| Dictionary, 500 keys | 7.4 | 0.9 | 18.3 | 21.7 | **67.7** (9.2x) |
| Dictionary, 2 key sets | 7.4 | 0.9 | – | 22.2 | **64.5** (8.7x) |
| Dictionary, 64 key sets | 7.0 | 0.9 | – | 16.1 | **18.0** (2.6x) |
| RemoteFunction | 8.1 | 12.0 | 92% failed | 55.9 | **88.7** (11x) |

**Bandwidth:** bytes per message on the network, with new random values in every message. Lower is better.

| Test | RemoteEvent | ByteNet Max | Blink | QuickNet | DogNet |
|---|---:|---:|---:|---:|---:|
| 1,000 booleans | 2,018 | 283 | 285 | 143 | **138** (15x) |
| 100 entities | 8,723 | 618 | 619 | 619 | **616** (14x) |
| 500 strings, all the same | 3,521 | 40 | 45 | 43 | **24** (147x) |
| 500 strings, all different | 5,419 | 4,028 | – | 3,381 | **3,366** (1.6x) |
| 2,000 numbers | 18,057 | 2,022 | 2,024 | 2,022 | **2,019** (8.9x) |
| Dictionary, 500 keys | 7,926 | 1,707 | 1,709 | 1,848 | **516** (15x) |
| Dictionary, 2 key sets | 6,918 | 1,852 | – | 1,734 | **508** (14x) |
| Dictionary, 64 key sets | 8,358 | 1,747 | – | 1,850 | **1,543** (5.4x) |
| RemoteFunction request | 4,531 | 533 | 519 | 520 | **516** (8.8x) |

## Why use it

 Roblox sends every value along with a description of its type and adds its own overhead to every RemoteEvent call. This is fine for a few messages a second, but it adds up when you send hundreds every frame, for example positions of many NPCs, bullets or inventory updates.

We fix that in three ways:

- **One call per frame.** Everything you send in a frame goes out together as one message, instead of one remote call each.
- **No wasted bytes.** Both sides already know what each message contains, so only the values are sent. A `true` takes 1 bit in a list, and a number from 0 to 255 takes 1 byte.
- **Checked and typed.** Data a client sends is checked before your code sees it. Your editor knows the type of every message, so autocomplete works.

In our tests DogNet was the fastest and smallest of the libraries we compared (ByteNet Max, Blink and QuickNet). See [Benchmarks](#benchmarks).

## Install

DogNet is one ModuleScript, `DogNet`, with a few ModuleScripts inside it. It must be in **ReplicatedStorage**, so the server and clients can both require it.

- **In Studio:** download [DogNet.rbxm](https://github.com/dogmastr/DogNet/releases/latest/download/DogNet.rbxm) from the latest release. In Studio, right-click **ReplicatedStorage**, choose **Insert from File...**, and pick the file.
- **With Rojo or Argon:** copy the `DogNet` folder into the folder you sync to ReplicatedStorage. Its `init.luau` becomes the `DogNet` ModuleScript, and the other files go inside it.

## Quick start

This example sends a message from a client to the server, from the server to every client, and asks the server a question. It takes three scripts.

### 1. Describe your messages

Create a **ModuleScript** named `Network` in **ReplicatedStorage**:

```lua
-- ReplicatedStorage > Network (ModuleScript)
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local DogNet = require(ReplicatedStorage.DogNet)

return DogNet.defineNamespace("Game", function()
	return {
		packets = {
			-- Client to server: a short message
			SayHello = DogNet.definePacket({
				value = DogNet.string,
			}),
			-- Server to every client: an announcement
			Announce = DogNet.definePacket({
				value = DogNet.string,
			}),
		},
		queries = {
			-- Client asks the server how many players are online
			GetPlayerCount = DogNet.defineQuery({
				request = DogNet.nothing,
				response = DogNet.uint16,
			}),
		},
	}
end)
```

### 2. The server

Create a **Script** in **ServerScriptService**:

```lua
-- ServerScriptService > Script
local Players = game:GetService("Players")
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local Network = require(ReplicatedStorage.Network)

-- Runs every time a client sends SayHello
Network.packets.SayHello.listen(function(message, player)
	print(player.Name, "says:", message)
	Network.packets.Announce.sendToAll(player.Name .. " said hello!")
end)

-- Answers GetPlayerCount. Whatever you return is the answer.
Network.queries.GetPlayerCount.listen(function()
	return #Players:GetPlayers()
end)
```

### 3. The client

Create a **LocalScript** in **StarterPlayer > StarterPlayerScripts**:

```lua
-- StarterPlayerScripts > LocalScript
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local Network = require(ReplicatedStorage.Network)

Network.packets.Announce.listen(function(text)
	print("Announcement:", text)
end)

Network.packets.SayHello.send("Hi from the client!")

local count = Network.queries.GetPlayerCount.invoke()
print("Players online:", count)
```

Press **Play**. The server output shows `Player1 says: Hi from the client!`, and the client output shows the announcement and `Players online: 1`.

## The four ideas

| Idea | What it is | Like |
|---|---|---|
| **Namespace** | A named group of packets and queries, defined in one ModuleScript. | A folder of RemoteEvents |
| **Packet** | A one-way message. Send it and move on. | A RemoteEvent |
| **Query** | A question that waits for an answer. | A RemoteFunction |
| **Data type** | What a packet or query carries, like `DogNet.uint8` or `DogNet.string`. | The arguments you'd pass to `FireServer` |

**Two rules for namespaces:**

1. **The server and the clients must both require the namespace module.** On a client, `require` waits for the server to define the namespace first. If the server never requires it, the client warns after 5 seconds and keeps waiting. The other way round, if the server sends a packet from a namespace a client hasn't required yet, that client holds all reliable data from the server until it does. So require your namespace modules early on both sides.
2. **Define it the same way on both sides.** Don't build a namespace differently depending on `RunService:IsServer()`. DogNet compares the two and errors if they don't match, so a mismatch can't silently scramble your data.

You can have as many namespaces as you like, for example one per feature (`Combat`, `Inventory`, `Chat`). Each needs its own unique name.

## Packets: sending messages

### Defining a packet

```lua
Damage = DogNet.definePacket({
	value = DogNet.struct({ target = DogNet.inst, amount = DogNet.uint16 }),
	reliabilityType = "reliable", -- optional; "reliable" is the default
}),
```

`value` is the [data type](#data-types) the packet carries. To send several values, wrap them in a `struct`.

### Sending

On a **client**:

| Function | Sends to |
|---|---|
| `packet.send(data)` | The server |

On the **server**:

| Function | Sends to |
|---|---|
| `packet.sendTo(data, player)` | One player |
| `packet.sendToAll(data)` | Every player |
| `packet.sendToList(data, players)` | Each player in a list |
| `packet.sendToAllExcept(data, player)` | Every player except one |

Sending doesn't go out right away. Everything sent during a frame goes out together at the end of that frame, in the order you sent it.

If `data` doesn't match the packet's type (a missing field, or a string where a number should be), the send errors and nothing is sent.

### Listening

These work on both the server and clients:

| Function | What it does |
|---|---|
| `packet.listen(callback)` | Calls `callback(data, player)` for every message. Returns a function that stops listening. |
| `packet.listenOnce(callback)` | Like `listen`, but only for the next message. |
| `packet.wait()` | Waits for the next message and returns `data, player`. |
| `packet.disconnectAll()` | Stops every listener on this packet. |

`player` is the player who sent the message when you're on the server. On a client it's `nil`, since messages there always come from the server.

```lua
local stop = Network.packets.Damage.listen(function(data, player)
	print(player.Name, "hit", data.target, "for", data.amount)
end)

-- Later, to stop listening:
stop()
```

A packet can have many listeners. A listener can yield (for example with `task.wait`). If one errors, the error shows in the output and other messages are still delivered.

**Messages don't get lost while your client loads.** On a client, messages that arrive before a packet has any listener are kept (up to 256) and delivered as soon as you connect one. This only applies until that packet's first listener connects.

### Reliable or unreliable

- **Reliable** (the default): every message arrives, in order. Use it for almost everything.
- **Unreliable**: a message may be lost or arrive out of order, but it can arrive sooner when the connection is poor. Use it for data you send again and again, where only the newest value matters, such as positions every frame.

An unreliable message must fit in about 950 bytes. A bigger one is sent reliably instead, with a warning. Messages from several unreliable packets are grouped into as few calls as fit under that limit.

## Queries: asking for an answer

### Defining a query

```lua
GetCoins = DogNet.defineQuery({
	request = DogNet.nothing,  -- what the asker sends
	response = DogNet.uint32,  -- what the answer is
	timeout = 5,               -- optional: seconds to wait (default 15)
}),
```

Use `DogNet.nothing` for a request or response that carries no data.

### Answering

```lua
-- Server
Network.queries.GetCoins.listen(function(request, player)
	return coins[player] or 0
end)
```

A query has **one** handler. Calling `listen` again replaces it (with a warning). The value the handler returns is the answer, and it must match the `response` type. The handler can yield, for example to load from a DataStore.

`listen` returns a function that removes the handler. `disconnectAll()` removes it too.

### Asking

```lua
-- Client
local coins = Network.queries.GetCoins.invoke()
```

`invoke` waits for the answer and returns it. If there is no answer, it **errors**, so wrap it in `pcall` when failing is possible:

```lua
local ok, result = pcall(Network.queries.GetCoins.invoke)
if ok then
	print("Coins:", result)
else
	warn("Couldn't get coins:", result)
end
```

`invoke` fails when:

- there's no answer before the timeout (15 seconds unless the query sets its own `timeout`),
- nothing is answering the query (no `listen` on the other side),
- the handler errored, or returned a value that doesn't match the response type,
- the client already has 32 questions waiting on the server (see `MAX_QUERIES_IN_FLIGHT` in [Settings](#settings)),
- the server was asking a player who left.

### The server asking a client

The server can ask one client, by passing the player:

```lua
local answer = Network.queries.GetSettings.invoke(nil, player)
```

Use this sparingly. A client can answer with anything or not answer at all, and the server waits for the timeout. Never set `timeout = math.huge` on a query the server uses to ask clients.

## Data types

A data type says what a value is, so DogNet can send just the value and nothing else. Pick the smallest type that fits your values.

### Numbers

| Type | Takes | Holds |
|---|---|---|
| `uint8` | 1 byte | Whole numbers 0 to 255 |
| `uint16` | 2 bytes | Whole numbers 0 to 65,535 |
| `uint32` | 4 bytes | Whole numbers 0 to about 4.29 billion |
| `int8` | 1 byte | Whole numbers -128 to 127 |
| `int16` | 2 bytes | Whole numbers -32,768 to 32,767 |
| `int32` | 4 bytes | Whole numbers from about -2.1 billion to 2.1 billion |
| `varuint` | 1 byte below 128, 2 below 16,384, and so on | Whole numbers 0 and up. Good for counts that are usually small but sometimes big. |
| `varint` | 1 byte from -64 to 63, then more | Like `varuint`, but also negative |
| `float16` | 2 bytes | Decimals, about 3 significant digits, up to 65,504. 1,000.3 comes back as 1,000.5. |
| `float32` | 4 bytes | Decimals, about 7 significant digits. Right for most decimals. |
| `float64` | 8 bytes | Any Luau number, exactly |

**Watch out:** a number that doesn't fit its type doesn't arrive as what you sent. 300 doesn't fit in a `uint8`, a negative number doesn't fit in a `uint` type, and 2.5 in any integer type loses the .5. When in doubt, go one size up.

### Text, flags and raw data

| Type | Takes | Holds |
|---|---|---|
| `bool` | 1 byte, or 1 bit in an array or struct | `true` or `false` |
| `string` | Its length (1 byte for under 128 characters) plus the text | Any string |
| `buff` | Its length plus its bytes | A `buffer` |

### Roblox values

| Type | Takes | Holds |
|---|---|---|
| `vec2` | 8 bytes | `Vector2` |
| `vec3` | 12 bytes | `Vector3`, full precision. Use it for positions. |
| `vec3f16` | 6 bytes | `Vector3` at `float16` precision. Good for directions, velocities and small offsets, not for positions. |
| `cframe` | 24 bytes | `CFrame`, full precision |
| `cframeCompact` | 18 bytes | `CFrame` with rotation accurate to about 0.005 degrees |
| `color3` | 3 bytes | `Color3`, 0 to 255 per channel |
| `inst` | No bytes of its own; Roblox sends a reference | An `Instance` (or `nil`) |
| `enumItem(Enum.X)` | 1, 2 or 4 bytes, depending on the Enum | An item of that Roblox Enum, e.g. `DogNet.enumItem(Enum.Material)` |

**Watch out with `inst`:** the other side must be able to see the instance. If it hasn't replicated yet, has streamed out, or lives somewhere clients can't see (like ServerStorage), it arrives as `nil`.

### Your own choices

`enum` takes a list of values and sends which one it is, as 1 byte (up to 256 values):

```lua
State = DogNet.definePacket({
	value = DogNet.enum({ "Idle", "Running", "Jumping" }),
}),

Network.packets.State.send("Running")
```

Sending a value that isn't in the list errors.

### Anything

| Type | Takes | Holds |
|---|---|---|
| `auto` | Picks the size from each value; a whole number 0 to 127 takes 1 byte | Numbers, strings, booleans, `nil`, tables, vectors, `Color3`, `CFrame`, buffers, Instances, and anything else a RemoteEvent can send, decided when you send |
| `unknown` | 1 byte, plus whatever Roblox charges for the value | Any value a RemoteEvent can send |
| `nothing` | 0 bytes | `nil`. For a query with no request or no response. |

`auto` is handy when you don't know the shape of the data in advance. It's bigger and slower than a fixed type, so prefer a real type when you know it.

### Combining types

| Type | Holds | Example |
|---|---|---|
| `struct({ ... })` | A table with fixed field names | `DogNet.struct({ hp = DogNet.uint8, name = DogNet.string })` |
| `array(type)` | A list (`{ 1, 2, 3 }`) | `DogNet.array(DogNet.vec3)` |
| `map(keyType, valueType)` | A dictionary | `DogNet.map(DogNet.string, DogNet.uint32)` |
| `optional(type)` | The type, or `nil` | `DogNet.optional(DogNet.string)` |

They nest:

```lua
Inventory = DogNet.definePacket({
	value = DogNet.struct({
		owner = DogNet.inst,
		gold = DogNet.uint32,
		items = DogNet.array(DogNet.struct({
			id = DogNet.uint16,
			count = DogNet.uint8,
			nickname = DogNet.optional(DogNet.string),
		})),
	}),
}),
```

Tips:

- **Use a `struct` when you know the field names in advance.** Field names are never sent, only the values. Fields not listed in the struct aren't sent at all.
- **Use a `map` when the keys vary**, like player names or item IDs.
- **Dictionaries with string keys get cheaper when they repeat.** When the server sends one player (with `sendTo`), or a client sends the server, a dictionary whose keys were sent before goes as a 1-byte reference plus its values. `sendToAll` doesn't do this, since a player who joins later wouldn't have the keys.
- **Arrays of `bool` are packed 8 per byte**, and so are `bool` and `optional` fields in a struct.

### Types in your editor

A packet's type follows from its definition. With `DogNet.struct({ hp = DogNet.uint8 })`, the editor knows `data.hp` is a number and autocompletes it.

To write the types out yourself, DogNet exports `DogNet.Packet<T>` and `DogNet.Query<Request, Response>`:

```lua
local DogNet = require(ReplicatedStorage.DogNet)
type HealthPacket = DogNet.Packet<{ hp: number }>
```

## Security: never trust the client

Exploiters can send anything a client can send, as often as they like. DogNet protects the parts it can:

- Data from a client is checked against the packet's type. Malformed data is thrown away, so your listener never sees it.
- Each player has a byte budget (a rate limit). Data over it is dropped with a warning in the output.
- A client can only have so many queries waiting on the server at once.

What DogNet can't check is whether the data **makes sense**. That's your job, in every listener and query handler on the server:

```lua
Network.packets.Damage.listen(function(data, player)
	local target = data.target
	-- Is it a real target? (inst can be nil or any Instance the client can see.)
	if not (target and target:IsDescendantOf(workspace.Enemies)) then
		return
	end
	-- Is the number reasonable? Don't let the client decide how hard it hits.
	local amount = math.min(data.amount, 50)
	-- ...also check range, cooldowns, whether the player is alive, etc.
end)
```

- Check that Instances are what you expect.
- Clamp numbers and check string lengths.
- Treat `auto` and `unknown` from clients as anything at all: check their type before using them.
- Filter any text a player typed before showing it to other players (with `TextService`), as Roblox requires.

## Settings

DogNet reads its settings from **attributes on the DogNet ModuleScript**. To change one in Studio: select the DogNet ModuleScript, find **Attributes** at the bottom of the Properties panel, click **Add Attribute**, and add a **Number** attribute with the name below. The defaults suit most games.

| Attribute | Default | What it does |
|---|---|---|
| `MAX_BUFFER_SIZE` | 8192 | The most bytes the server accepts from one client in one frame. Also the size of each player's byte budget. |
| `RATE_LIMIT` | Same as `MAX_BUFFER_SIZE` | Bytes per second that each player's budget refills. |
| `UNRELIABLE_LIMIT` | 950 | Unreliable messages are grouped to stay under this many bytes, the most Roblox delivers. |
| `QUERY_TIMEOUT` | 15 | Seconds `invoke` waits, unless the query sets its own `timeout`. |
| `MAX_QUERIES_IN_FLIGHT` | 32 | How many questions one client can have waiting on the server at once. |
| `KEY_SET_MEMORY` | 262144 | Roughly how many bytes each side may use per player to remember dictionary keys. The server's value is used on both sides. 0 turns it off. |

DogNet reads them once, the first time it's required, so set them in Studio before you press Play.

If a client legitimately sends more than 8 KB in one frame (the output warns you in Studio) or more than 8 KB a second on average, raise `MAX_BUFFER_SIZE`. `RATE_LIMIT` follows it unless you set it too.

## Troubleshooting

Every DogNet message in the output starts with `[DogNet]`.

| Message | Why | Fix |
|---|---|---|
| `namespace "X" is defined differently on the server and the client` | The two sides built different definitions. | Require the same ModuleScript on both sides, and don't make definitions depend on `IsServer()`. |
| `still waiting for the server to define namespace "X"` | A client required a namespace the server hasn't. | Require the module from a server Script too. |
| `namespace "X" is defined twice` | Two `defineNamespace` calls use the same name. | Give each namespace its own name. (Requiring the same module twice is fine.) |
| `received data for namespace "X", which this client hasn't defined` | The server sent a packet from a namespace this client hasn't required. Reliable data from the server waits until it does. | Require every namespace module early on the client, for example at the top of a LocalScript in StarterPlayerScripts. |
| `can't use "send" on a definition` | You called a function on what `definePacket` or `defineQuery` returned. | Use the packet from the module: `Network.packets.Name.send(...)`. |
| `send is client-only` or `... are server-only` | The server called `send`, or a client called `sendTo`, `sendToAll`, `sendToList` or `sendToAllExcept`. | On the server use `sendTo` and friends; on a client use `send`. |
| `invoke on the server needs the player to ask` | The server called `invoke` without a player. | Use `invoke(request, player)`. |
| `invoke failed: no response within N seconds` | Nothing answered in time. | Make sure the other side calls `listen` on the query and that the handler returns. |
| `invoke failed: nothing is handling it` | The other side has no handler for this query. | Call `listen` on the query on the other side. |
| `invoke failed: its handler errored` | The handler hit an error. | The handler's own error is in the output of the side that answered (`... handler errored: ...`). |
| `... returned a value that doesn't match the response type` | The handler returned the wrong kind of value, or nothing when a value was expected. | Return a value of the `response` type. |
| `already had a handler and this one replaces it` | `listen` was called twice on one query. | A query has one handler: combine them, or disconnect the old one first. |
| `got more than 256 messages before anything listened` | A client received many messages before it connected a listener for that packet. | Connect the listener earlier, or send less before it's ready. |
| `an unreliable message ... is over the 950 byte limit` | One unreliable message was too big for Roblox. | Send less at once, or make the packet reliable. It was sent reliably this time. |
| `this client sent N bytes in one frame` | A client sent more in one frame than the server accepts. | Send less per frame, or raise `MAX_BUFFER_SIZE`. |
| `dropped data from Player: rate limit exceeded` | A player sent more than their byte budget. | Normal for exploiters. If a real player hits it, raise `RATE_LIMIT`. |
| `dropped data from Player: malformed batch` | Data from a player didn't match its types. | Usually an exploiter. If it happens in normal play, check both sides use the same DogNet version. |

## How it works

If you're curious:

- **One remote call per frame.** Every message sent in a frame is written into one buffer (a block of raw bytes), and the buffer is fired once, or once per player on the server.
- **Only the values.** Both sides know each message's type from its definition, so no type information is sent. Booleans are packed 32 to a write, and small numbers take 1 byte.
- **Written for Luau's native code generator.** The code that reads and writes messages is shaped for what native code runs fastest.
- **Laid out for Roblox's compression.** Roblox compresses every remote call. DogNet orders bytes so they compress well, sends a run of equal strings once, and sends repeated dictionary keys as a 1-byte reference.
- **Queries ride along.** Questions and answers go in the same buffers as packets instead of using RemoteFunctions.

## Coming from ByteNet Max

Most code works after changing the `require`. The differences:

- `send` is client-only. On the server use `sendTo`, `sendToAll`, `sendToList` or the new `sendToAllExcept`.
- `listen` and `listenOnce` return a function that disconnects, and `wait` also returns the sender.
- A query has one handler, and there is no `listenOnce` for queries.
- `playerName` and `playerIdentifier` aren't included. Use `inst` for a Player, or `string`.
- New types: `float16`, `varuint`, `varint`, `vec3f16`, `cframeCompact`, `enum` and `enumItem`.
- The server and clients must both use DogNet: it can't talk to ByteNet Max.
