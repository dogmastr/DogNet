--!strict
--!native
--!optimize 2

--[[
	Serialization core: every data type ("codec") lives in this one module on
	purpose. Writers and readers share the buffer and cursor as upvalues instead
	of passing them around or looking them up in tables, which is what keeps
	the hot path cheap.

	Speed comes from making as few Lua function calls as possible, since on
	Roblox a call costs far more than the work inside a small codec:
	  - A packet's send function writes the message header and commits it
	    itself, so a send is one call plus the codec's write.
	  - Structs are compiled into one loop per field type (all u8 fields, all
	    f32 fields, ...) instead of a branch per field, and arrays of fixed-size
	    structs, primitives and strings run their items inline.
	  - Received messages go straight from the codec to the listeners: no
	    wrapper per message and no thread per listener call (see `process`).

	Writing
		A Channel owns a growable buffer plus a "refs" array for values that
		can't live in a buffer (Instances, arbitrary Roblox values). The channel
		being written stays loaded in the upvalues between messages; its table
		holds the committed cursor, which only moves once a message has been
		written in full. If a codec errors midway, the next message starts from
		the committed cursor again, so a bad `send` can't corrupt the batch.

	Reading
		`process` walks a received batch and hands each message to the reader
		registered for its id, then to that id's listeners. Refs are consumed in
		the same order they were written, so they cost zero bytes in the buffer.

	Remembering
		A reliable stream to one peer carries state from message to message:
		the key sets of string-keyed maps (see "Remembered key sets"), so a map
		whose keys were sent before costs a one-byte reference plus its values.

	Codec contract
		size     exact encoded size, when every value encodes to the same size
		writeRaw present when `size` is; the caller must have reserved `size` bytes
		minSize  smallest possible encoding, used to bound counts from clients
]]

export type Channel = {
	buff: buffer,
	size: number,
	cursor: number,
	refs: { any },
	refCount: number,
	-- Key sets the receiver remembers, on a reliable channel to one peer.
	shapes: Shapes?,
	-- Array codecs (their count keys) with a count written here without
	-- remembering it. See `passCounts`.
	countKeys: { [any]: boolean }?,
}

--[[
	Remembered key sets

	A reliable stream to one peer (the client's to the server, and the
	server's to each player) remembers the key sets of the string-keyed maps
	sent on it, and each array codec's last count. Both ends keep the same table: the writer marks each map whose
	key set it adds, and both ends apply the same rule for what to forget (the
	oldest first, to stay within SHAPE_SLOTS key sets and the memory budget),
	in the order the stream carries them. A map whose keys are remembered then
	goes as a reference to them plus its values.

	A reader that misses part of its stream (the server drops a batch over the
	rate limit, say) can't trust its table any more. It refuses references
	from then on and asks the writer to reset (a control message); the writer
	forgets everything and marks the place in the stream where it did, and
	the reader starts over from that mark. Messages in between that use a
	reference are lost, which only happens where data was already being lost.
]]
export type Shapes = {
	-- Bytes allowed and used: each key set counts its key bytes plus SHAPE_KEY_COST per key.
	memory: number,
	used: number,
	-- Key sets added so far, and the number of the oldest still remembered.
	-- Number n lives at index n % SHAPE_SLOTS + 1.
	added: number,
	oldest: number,
	keys: { { string } | false },
	counts: { number },
	sizes: { number },
	-- Writer: indexes by the first key of their key set, most recent last.
	byFirst: { [string]: { number } },
	-- Writer: the channel the key sets go out on.
	channel: Channel?,
	-- Reader: a table with exactly each key set's keys, cloned for every map read with it.
	templates: { [number]: { [string]: any } },
	-- Reader: part of the stream was lost, so references mean nothing until the writer resets.
	desynced: boolean,
	-- The count of each array codec's last array on this stream (see writeArrayCount).
	lastCounts: { [any]: number },
}

export type Codec = {
	kind: string,
	desc: string,
	size: number?,
	minSize: number,
	write: (value: any) -> (),
	writeRaw: ((value: any) -> ())?,
	read: () -> any,
	inner: Codec?,
	-- Set on plain numeric/vector codecs so structs and arrays can read and write them in place.
	prim: number?,
	-- Set on fixed-size structs: write or read `count` of them back to back, inline.
	writeMany: ((list: { any }, count: number) -> ())?,
	readMany: ((list: { any }, count: number) -> ())?,
}

-- What a message id maps to when reading: the function that reads its payload
-- (and, for queries, acts on it; codec reads ignore the sender), and the
-- listeners the value goes to.
export type Reader = (sender: Player?) -> any
export type Listener = (value: any, sender: Player?) -> ()

-- Roblox charges roughly this many bytes per ref on an UnreliableRemoteEvent.
local REF_COST = 6
-- Zero-byte items (e.g. Instances in an array) one batch from a client may
-- hold when `process` is given no allowance, and repeats in one string array.
local MAX_ZERO_SIZE_COUNT = 4096
local AUTO_MAX_DEPTH = 32
local SQRT2 = math.sqrt(2)
local INV_SQRT2 = 1 / SQRT2
-- Key sets one stream remembers at most. A reference is one byte below 123.
local SHAPE_SLOTS = 64
-- Rough memory a remembered key costs beyond its bytes: its place in the key
-- list and in the reader's template table.
local SHAPE_KEY_COST = 40

-- Message id 0 is the transport's own, followed by one of these.
local CONTROL_ID = 0
-- The writer forgot every key set at this point in the stream; so must the reader.
local CONTROL_RESET = 0
-- The reader lost track of the key sets: please reset.
local CONTROL_RESYNC = 1

local Serdes = {}

Serdes.REF_COST = REF_COST
Serdes.CONTROL_ID = CONTROL_ID

--------------------------------------------------------------------------------
-- Write state
--------------------------------------------------------------------------------

local wchannel: Channel? = nil
local wbuff: buffer = buffer.create(0)
local wsize = 0
local wcursor = 0
local wrefs: { any } = {}
local wrefCount = 0
local wshapes: Shapes? = nil
-- Key sets the message being written added to, until it's committed. If it
-- errors instead, the next message resets them (see `recover`).
local wtouched: Shapes? = nil

local function grow(bytes: number)
	local needed = wcursor + bytes
	local newSize = math.max(wsize * 2, 64)
	while newSize < needed do
		newSize *= 2
	end
	local newBuff = buffer.create(newSize)
	buffer.copy(newBuff, 0, wbuff, 0, wcursor)
	wbuff = newBuff
	wsize = newSize
	local channel = wchannel :: Channel
	channel.buff = newBuff
	channel.size = newSize
end

-- Makes `channel` the one being written, starting at its committed cursor.
local function begin(channel: Channel)
	if wchannel ~= channel then
		wchannel = channel
		wbuff = channel.buff
		wsize = channel.size
		wrefs = channel.refs
		wshapes = channel.shapes
	end
	wcursor = channel.cursor
	wrefCount = channel.refCount
end

function Serdes.channel(): Channel
	return {
		buff = buffer.create(256),
		size = 256,
		cursor = 0,
		refs = {},
		refCount = 0,
		shapes = nil,
		countKeys = nil,
	}
end

local function newShapes(memory: number): Shapes
	return {
		memory = memory,
		used = 0,
		added = 0,
		oldest = 0,
		keys = table.create(SHAPE_SLOTS, false),
		counts = table.create(SHAPE_SLOTS, 0),
		sizes = table.create(SHAPE_SLOTS, 0),
		byFirst = {},
		channel = nil,
		templates = {},
		desynced = false,
		lastCounts = {},
	}
end

-- Makes `channel` remember key sets for its receiver, which must read it with
-- `Serdes.shapes` of the same `memory`. Only for a reliable channel to one peer.
function Serdes.remember(channel: Channel, memory: number)
	local shapes = newShapes(memory)
	shapes.channel = channel
	channel.shapes = shapes
	if wchannel == channel then
		wshapes = shapes
	end
end

-- The reading side of a channel that remembers key sets.
function Serdes.shapes(memory: number): Shapes
	return newShapes(memory)
end

local function forgetShape(shapes: Shapes, index: number)
	local keys = shapes.keys[index]
	if keys then
		shapes.keys[index] = false
		shapes.used -= shapes.sizes[index]
		shapes.templates[index] = nil
		local first = keys[1]
		local list = shapes.byFirst[first]
		if list then
			local at = table.find(list, index)
			if at then
				table.remove(list, at)
			end
			if #list == 0 then
				shapes.byFirst[first] = nil
			end
		end
	end
end

-- Adds a key set and returns its index, forgetting the oldest until it fits.
-- Both ends run this in the same order. Callers check `size <= memory`.
local function addShape(shapes: Shapes, keys: { string }, count: number, size: number): number
	local number = shapes.added
	shapes.added = number + 1
	while number + 1 - shapes.oldest > SHAPE_SLOTS or shapes.used + size > shapes.memory do
		forgetShape(shapes, shapes.oldest % SHAPE_SLOTS + 1)
		shapes.oldest += 1
	end
	local index = number % SHAPE_SLOTS + 1
	shapes.keys[index] = keys
	shapes.counts[index] = count
	shapes.sizes[index] = size
	shapes.used += size
	return index
end

local function clearShapes(shapes: Shapes)
	shapes.used = 0
	shapes.added = 0
	shapes.oldest = 0
	for index = 1, SHAPE_SLOTS do
		shapes.keys[index] = false
	end
	table.clear(shapes.byFirst)
	table.clear(shapes.templates)
	table.clear(shapes.lastCounts)
	shapes.desynced = false
end

-- Writes a whole control message into `channel` and commits it.
local function writeControl(channel: Channel, code: number)
	begin(channel)
	if wcursor + 2 > wsize then
		grow(2)
	end
	buffer.writeu8(wbuff, wcursor, CONTROL_ID)
	buffer.writeu8(wbuff, wcursor + 1, code)
	wcursor += 2
	channel.cursor = wcursor
	channel.refCount = wrefCount
end

-- Forgets the key sets `channel` remembers and tells its reader to do the same.
function Serdes.resetShapes(channel: Channel)
	local shapes = channel.shapes
	if shapes then
		if wtouched == shapes then
			wtouched = nil
		end
		clearShapes(shapes)
		writeControl(channel, CONTROL_RESET)
	end
end

-- Asks whoever reads `channel` to reset the key sets it sends this side.
function Serdes.requestReset(channel: Channel)
	writeControl(channel, CONTROL_RESYNC)
end

-- For a reader that missed part of its stream: refuse references from now
-- on. Returns true the first time, when the writer should be asked to reset.
function Serdes.lostTrack(shapes: Shapes): boolean
	if shapes.desynced then
		return false
	end
	shapes.desynced = true
	return true
end

-- The previous message errored after adding key sets it never delivered, so
-- the writer's table is ahead of the reader's. Called before writing the next
-- message: resetting both is simpler than undoing.
local function recover()
	local shapes = wtouched :: Shapes
	wtouched = nil
	Serdes.resetShapes(shapes.channel :: Channel)
end

-- Lower-level alternative to `message`: `load`, write with a codec, `save`.
function Serdes.load(channel: Channel)
	if wtouched then
		recover()
	end
	begin(channel)
end

function Serdes.save()
	local channel = wchannel :: Channel
	channel.cursor = wcursor
	channel.refCount = wrefCount
	wtouched = nil
end

-- Exact-size buffers handed out by `dump`, by size. Roblox copies a remote's
-- arguments when it's fired, so a buffer can be refilled for the next batch
-- of the same size (steady traffic repeats sizes) instead of allocating, and
-- later collecting, a new one every frame.
local DUMP_CACHE_SIZE = 8
local dumpCache: { [number]: buffer } = {}
local dumpCacheCount = 0

-- Returns an exact-size copy of the channel's bytes plus its refs (or nil).
-- Both are only valid until the next `dump` or `reset`: fire them first.
function Serdes.dump(channel: Channel): (buffer, { any }?)
	local cursor = channel.cursor
	local out = dumpCache[cursor]
	if out == nil then
		out = buffer.create(cursor)
		if dumpCacheCount >= DUMP_CACHE_SIZE then
			table.clear(dumpCache)
			dumpCacheCount = 0
		end
		dumpCache[cursor] = out
		dumpCacheCount += 1
	end
	buffer.copy(out, 0, channel.buff, 0, cursor)

	local refCount = channel.refCount
	local refs = channel.refs
	-- A message that errored after pushing refs leaves entries past refCount.
	for index = #refs, refCount + 1, -1 do
		refs[index] = nil
	end

	return out, if refCount > 0 then refs else nil
end

function Serdes.reset(channel: Channel)
	-- Let go of a buffer a one-off burst grew, once traffic is back to normal.
	if channel.size > 65536 and channel.cursor < channel.size // 4 then
		channel.buff = buffer.create(256)
		channel.size = 256
		if wchannel == channel then
			wchannel = nil
		end
	end
	channel.cursor = 0
	channel.refCount = 0
	channel.countKeys = nil
	if next(channel.refs) ~= nil then
		table.clear(channel.refs)
	end
end

-- Bytes the channel would cost on an UnreliableRemoteEvent.
function Serdes.cost(channel: Channel): number
	return channel.cursor + channel.refCount * REF_COST
end

-- Adds to `total` the least Roblox's encoding of `value` takes beyond its
-- type: string and buffer contents, and two bytes per table entry plus what
-- its keys and values take. Stops adding once past `limit`. Nil for tables
-- nested deeper than `auto` allows.
local function addRefContents(value: any, total: number, limit: number, depth: number): number?
	local kind = type(value)
	if kind == "string" then
		return total + string.len(value)
	elseif kind == "buffer" then
		return total + buffer.len(value)
	elseif kind ~= "table" then
		return total
	elseif depth >= AUTO_MAX_DEPTH then
		return nil
	end
	for key, entry in value do
		if total > limit then
			break
		end
		local sum = addRefContents(key, total + 2, limit, depth + 1)
		if sum == nil then
			return nil
		end
		sum = addRefContents(entry, sum, limit, depth + 1)
		if sum == nil then
			return nil
		end
		total = sum
	end
	return total
end

--[[
	Roughly what refs from a client took to send, to charge against its byte
	budget along with the batch: REF_COST per ref, plus the contents of any
	strings, buffers or tables among them (`unknown` passes on anything
	Roblox can send). Stops counting once past `limit`, so checking a huge
	table costs no more than the budget allows. Nil if they can't be refs
	DogNet wrote: a key that isn't a number, or tables nested deeper than
	`auto` allows.
]]
function Serdes.refsCost(refs: { any }, limit: number): number?
	local total = 0
	for key, value in refs do
		if type(key) ~= "number" then
			return nil
		end
		local sum = addRefContents(value, total + REF_COST, limit, 0)
		if sum == nil then
			return nil
		end
		total = sum
		if total > limit then
			break
		end
	end
	return total
end

--[[
	A reader that remembers array counts stores every count it reads, also
	those written without remembering: a broadcast, or a message encoded once
	for several players, spliced into a player's stream or fired to them as
	is. The writer that remembers for that reader never saw those counts, so
	it would send "same count" for a length the reader no longer has. When
	such bytes reach a reader, the channel that remembers for it forgets the
	counts of those arrays and sends their next counts in full.
]]
local function passCounts(target: Channel, keys: { [any]: boolean })
	local shapes = target.shapes
	if shapes then
		local counts = shapes.lastCounts
		for key in keys do
			counts[key] = nil
		end
	else
		-- Passed on again if these bytes go further (a broadcast spliced in).
		local own = target.countKeys
		if own == nil then
			own = {}
			target.countKeys = own
		end
		for key in keys do
			own[key] = true
		end
	end
end

-- `source`'s bytes reached `target`'s reader another way than through
-- `target` (a broadcast batch fired to that player as is): see `passCounts`.
function Serdes.shareCounts(target: Channel, source: Channel)
	local keys = source.countKeys
	if keys then
		passCounts(target, keys)
	end
end

-- Appends bytes [from, to) and refs (refFrom, refTo] of `source` to `target`.
function Serdes.append(target: Channel, source: Channel, from: number, to: number, refFrom: number, refTo: number)
	local keys = source.countKeys
	if keys then
		passCounts(target, keys)
	end
	begin(target)
	local length = to - from
	if wcursor + length > wsize then
		grow(length)
	end
	buffer.copy(wbuff, wcursor, source.buff, from, length)
	wcursor += length

	local sourceRefs = source.refs
	for index = refFrom + 1, refTo do
		wrefCount += 1
		wrefs[wrefCount] = sourceRefs[index]
	end
	target.cursor = wcursor
	target.refCount = wrefCount
end

-- Message header: id (1 byte below 128, else 2 bytes), then a call id for
-- query traffic (a varint: call ids are reused smallest first, so usually one
-- byte), then the payload. Writes at the cursor of the loaded channel.
function Serdes.writeMessage(id: number, callId: number?, write: (any) -> (), value: any)
	if wcursor + 12 > wsize then
		grow(12)
	end
	if id < 128 then
		buffer.writeu8(wbuff, wcursor, id)
		wcursor += 1
	else
		buffer.writeu8(wbuff, wcursor, 128 + id // 256)
		buffer.writeu8(wbuff, wcursor + 1, id % 256)
		wcursor += 2
	end
	if callId then
		while callId >= 128 do
			buffer.writeu8(wbuff, wcursor, callId % 128 + 128)
			wcursor += 1
			callId //= 128
		end
		buffer.writeu8(wbuff, wcursor, callId)
		wcursor += 1
	end
	write(value)
end

-- Writes one whole message into `channel` and commits it.
function Serdes.message(channel: Channel, id: number, callId: number?, write: (any) -> (), value: any)
	if wtouched then
		recover()
	end
	begin(channel)
	Serdes.writeMessage(id, callId, write, value)
	channel.cursor = wcursor
	channel.refCount = wrefCount
	wtouched = nil
end

-- A send function for one message id into one channel: the fast path behind
-- packet sends, with the header, the codec and the commit in a single call.
function Serdes.sender(channel: Channel, id: number, codec: Codec): (value: any) -> ()
	local write = codec.write
	local writeRaw = codec.writeRaw
	local size = codec.size

	if id >= 128 then
		local high, low = 128 + id // 256, id % 256
		return function(value: any)
			if wtouched then
				recover()
			end
			begin(channel)
			if wcursor + 2 > wsize then
				grow(2)
			end
			buffer.writeu8(wbuff, wcursor, high)
			buffer.writeu8(wbuff, wcursor + 1, low)
			wcursor += 2
			write(value)
			channel.cursor = wcursor
			channel.refCount = wrefCount
			wtouched = nil
		end
	end

	if writeRaw and size then
		-- Fixed size: one bounds check covers the header and the payload.
		local total = 1 + size
		return function(value: any)
			if wtouched then
				recover()
			end
			if wchannel ~= channel then
				begin(channel)
			end
			local cursor = channel.cursor
			wrefCount = channel.refCount
			wcursor = cursor
			if cursor + total > wsize then
				grow(total)
			end
			buffer.writeu8(wbuff, cursor, id)
			wcursor = cursor + 1
			writeRaw(value)
			channel.cursor = wcursor
			channel.refCount = wrefCount
		end
	end

	return function(value: any)
		if wtouched then
			recover()
		end
		if wchannel ~= channel then
			begin(channel)
		end
		local cursor = channel.cursor
		wrefCount = channel.refCount
		wcursor = cursor
		if cursor + 1 > wsize then
			grow(1)
		end
		buffer.writeu8(wbuff, cursor, id)
		wcursor = cursor + 1
		write(value)
		channel.cursor = wcursor
		channel.refCount = wrefCount
		wtouched = nil
	end
end

--------------------------------------------------------------------------------
-- Read state
--------------------------------------------------------------------------------

local rbuff: buffer = buffer.create(0)
local rlength = 0
local rcursor = 0
local rrefs: { any }? = nil
local rrefIndex = 0
local rtrusted = true
local rsender: Player? = nil
-- Key sets the sender's stream remembers, if the batch came on one.
local rshapes: Shapes? = nil
-- Zero-byte items (and repeats in string arrays) the untrusted batch being
-- read may still hold. They take no room in the batch, so its length can't
-- bound them; `process` sets this.
local rzeroLeft = 0
local rreaders: { [number]: Reader } = {}
local rtargets: { [number]: { Listener } } = {}

-- Where the batch stopped on an id with no reader.
local stopAt: number? = nil
local stopRef: number? = nil
local stopId: number? = nil
local finished = false

--[[
	Listeners are called directly from the loop reading the batch, so a
	message costs no thread switch. Where that loop runs depends on who sent
	the batch:

	Untrusted (the server reading a client's batch): `process` runs it on a
	reader thread it resumes and reuses from batch to batch, so bad data
	can't take down the caller. If a listener yields or errors, `process`
	gives up on that reader and starts a new one where it stopped (the next
	listener of the same message, then the rest of the batch). The old reader
	finishes the listener whenever it resumes and then ends, without touching
	the batch again. A listener that yields holds nothing else up.

	Trusted (the client reading the server's batch): `readTrusted` runs it on
	the caller's own thread, with no thread switch or pcall at all, which on
	Roblox costs more than reading a whole small batch. If a listener yields
	or errors (Roblox reports the error as usual), the batch is "stalled" at
	that point; the client picks it back up with `continueStalled` before it
	reads anything else, and on the next Heartbeat, so what's left arrives
	later in the same frame.

	Either way, `readerGeneration` changes whenever a batch is taken from
	whoever was reading it, and a loop that finds it changed after a listener
	returns stops without touching the batch again.
]]
-- Which listener the reader is in: 0 for none (reading), -1 for the only
-- listener of its message, or an index into `dispatchList` (several listeners,
-- `dispatchValue` being the message).
local dispatchIndex = 0
local dispatchList: { Listener }? = nil
local dispatchValue: any = nil
local readerGeneration = 0
local idleReader: thread? = nil
local trustedStalled = false
-- Only `process` resumes readers with this, so code that grabs a reader with
-- coroutine.running() and resumes it later can't make it read.
local RESUME = table.freeze({})

-- Reads the rest of the current batch. Returns false if another reader took
-- the batch over while a listener was running (so this one must stop).
local function readLoop(generation: number): boolean
	local sender = rsender

	-- Finish the message a previous reader stopped in.
	if dispatchIndex > 0 then
		local pending = dispatchList :: { Listener }
		local value = dispatchValue
		for index = dispatchIndex + 1, #pending do
			dispatchIndex = index
			pending[index](value, sender)
			if readerGeneration ~= generation then
				return false
			end
		end
	end
	dispatchIndex = 0

	local buff, readers, targets, length = rbuff, rreaders, rtargets, rlength
	while rcursor < length do
		local start = rcursor
		local id = buffer.readu8(buff, start)
		if id < 128 then
			rcursor = start + 1
		else
			id = (id - 128) * 256 + buffer.readu8(buff, start + 1)
			rcursor = start + 2
		end

		local read = readers[id]
		if read == nil then
			stopAt, stopRef, stopId = start, rrefIndex, id
			break
		end
		local value = read(sender)
		-- A query's reader runs its handler, which may have yielded, and the
		-- batch moved on to another reader meanwhile.
		if readerGeneration ~= generation then
			return false
		end

		local listeners = targets[id]
		if listeners then
			local count = #listeners
			if count == 1 then
				-- The usual case. If it yields, this message has nothing left to resume.
				dispatchIndex = -1
				listeners[1](value, sender)
				if readerGeneration ~= generation then
					return false
				end
				dispatchIndex = 0
			elseif count > 1 then
				dispatchList = listeners
				dispatchValue = value
				for index = 1, count do
					dispatchIndex = index
					listeners[index](value, sender)
					if readerGeneration ~= generation then
						return false
					end
				end
				dispatchIndex = 0
			end
		end
	end
	finished = true
	return true
end

-- The body of every reader thread: parks, and reads a batch each time
-- `process` resumes it. Returning ends the thread, which happens only when it
-- was given up on while a listener yielded.
local function readerLoop(token: any, generation: number)
	while true do
		if token == RESUME and not readLoop(generation) then
			return
		end
		token, generation = coroutine.yield()
	end
end

local function reportListenerError(thread: thread, problem: any)
	if type(problem) == "string" then
		task.spawn(error, debug.traceback(thread, problem), 0)
	else
		task.spawn(error, problem, 0)
	end
end

local function startBatch(
	buff: buffer,
	refs: { any }?,
	sender: Player?,
	readers: { [number]: Reader },
	targets: { [number]: { Listener } },
	trusted: boolean,
	cursor: number,
	refIndex: number,
	shapes: Shapes?
)
	if trustedStalled then
		error("[DogNet] a batch was read while another was stalled; continueStalled must run first", 0)
	end
	rbuff = buff
	rlength = buffer.len(buff)
	rcursor = cursor
	rrefs = refs
	rrefIndex = refIndex
	rtrusted = trusted
	rsender = sender
	rreaders = readers
	rtargets = targets
	rshapes = shapes
	stopAt, stopRef, stopId = nil, nil, nil
	dispatchIndex = 0
	finished = false
end

-- Don't keep the batch or its values alive.
local function endBatch()
	rrefs = nil
	rsender = nil
	rshapes = nil
	dispatchList = nil
	dispatchValue = nil
end

--[[
	Reads messages from `buff` starting at `cursor`, on a reader thread: each
	id's reader, then its listeners. Returns nothing when the batch is fully
	consumed. On an id with no reader it stops and returns where that message
	starts, the ref index at that point and the id, so the caller can drop or
	retry the rest. If the data can't be read, the fourth result is the error.
	Listener errors are reported and don't stop the batch. `shapes` is the
	key sets of the stream the batch came on, if it remembers any.
	`zeroAllowance` is how many zero-byte items (Instances in an array, say,
	or repeats in a string array) an untrusted batch may hold in all,
	MAX_ZERO_SIZE_COUNT if not given; the fifth result is how many it read,
	for the caller to charge.
]]
function Serdes.process(
	buff: buffer,
	refs: { any }?,
	sender: Player?,
	readers: { [number]: Reader },
	targets: { [number]: { Listener } },
	trusted: boolean,
	cursor: number,
	refIndex: number,
	shapes: Shapes?,
	zeroAllowance: number?
): (number?, number?, number?, any, number)
	-- A stalled trusted batch keeps its place: set it aside and put it back after.
	local stalled: { any }? = nil
	if trustedStalled then
		stalled = table.pack(
			rbuff,
			rlength,
			rcursor,
			rrefs,
			rrefIndex,
			rsender,
			rreaders,
			rtargets,
			dispatchList,
			dispatchIndex,
			dispatchValue,
			rshapes
		)
		trustedStalled = false
	end
	startBatch(buff, refs, sender, readers, targets, trusted, cursor, refIndex, shapes)
	local zeroAllowed = math.max(zeroAllowance or MAX_ZERO_SIZE_COUNT, 0)
	rzeroLeft = zeroAllowed

	local failure: any = nil
	while true do
		local thread = idleReader
		if thread then
			idleReader = nil
		else
			thread = coroutine.create(readerLoop)
		end
		local ok, problem = coroutine.resume(thread :: thread, RESUME, readerGeneration)
		if finished then
			-- It read to the end and parked itself again, ready for the next batch.
			if ok then
				idleReader = thread
			end
			break
		end

		-- The reader stopped early. Whatever it was doing, it's on its own now.
		readerGeneration += 1
		if not ok then
			if dispatchIndex == 0 then
				-- Reading failed, not a listener.
				failure = problem
				break
			end
			reportListenerError(thread :: thread, problem)
		end
	end

	endBatch()
	local at, ref, id = stopAt, stopRef, stopId
	if stalled then
		rbuff, rlength, rcursor, rrefs, rrefIndex, rsender, rreaders, rtargets, dispatchList, dispatchIndex, dispatchValue, rshapes =
			table.unpack(stalled, 1, stalled.n)
		rtrusted = true
		stopAt, stopRef, stopId = nil, nil, nil
		finished = false
		trustedStalled = true
	end
	-- No more than the allowance, even if the batch asked for more.
	local zeroRead = zeroAllowed - math.max(rzeroLeft, 0)
	if failure ~= nil then
		return nil, nil, nil, failure, zeroRead
	end
	return at, ref, id, nil, zeroRead
end

--[[
	Reads a batch the server sent on the calling thread, like `process` but
	with no thread switch. Returns like `process`, except that the last result
	says whether the batch was taken over: a listener that yields or errors,
	or data that fails to read, leaves the batch stalled, and whatever called
	this never gets control back unless the listener resumes, by which time
	`continueStalled` has usually taken the batch. The caller must then leave
	everything alone.
]]
function Serdes.readTrusted(
	buff: buffer,
	refs: { any }?,
	readers: { [number]: Reader },
	targets: { [number]: { Listener } },
	cursor: number,
	refIndex: number,
	shapes: Shapes?
): (number?, number?, number?, boolean)
	startBatch(buff, refs, nil, readers, targets, true, cursor, refIndex, shapes)
	trustedStalled = true
	local generation = readerGeneration + 1
	readerGeneration = generation
	if not readLoop(generation) then
		return nil, nil, nil, true
	end
	trustedStalled = false
	endBatch()
	return stopAt, stopRef, stopId, false
end

-- Whether a `readTrusted` batch stopped partway (see `continueStalled`).
function Serdes.stalled(): boolean
	return trustedStalled
end

--[[
	Picks a stalled batch up where it stopped, on the calling thread: the next
	listener of the message it was on, then the rest of the batch. Returns the
	batch, its refs, where it stopped (like `readTrusted`), whether reading
	failed, and whether it was taken over again (like `readTrusted`). If it
	stalled while reading a message rather than in a listener, that data
	couldn't be read: the rest of the batch is dropped and `failed` is true.
]]
function Serdes.continueStalled(): (buffer, { any }?, number?, number?, number?, boolean, boolean)
	-- Whoever was reading it (a thread suspended in a listener) stops once it resumes.
	local generation = readerGeneration + 1
	readerGeneration = generation
	local buff, refs = rbuff, rrefs
	if dispatchIndex == 0 then
		trustedStalled = false
		endBatch()
		return buff, refs, nil, nil, nil, true, false
	end
	stopAt, stopRef, stopId = nil, nil, nil
	if not readLoop(generation) then
		return buff, refs, nil, nil, nil, false, true
	end
	trustedStalled = false
	endBatch()
	return buff, refs, stopAt, stopRef, stopId, false, false
end

local trustedWaiting = false

-- The client sets this while it has batches waiting (held for a namespace),
-- so the receiver below knows to queue new ones behind them.
function Serdes.setWaiting(waiting: boolean)
	trustedWaiting = waiting
end

--[[
	The client's handler for reliable batches. It's built here so that reading
	a batch runs entirely in this module: on Roblox, the first call into each
	module (and each function) in a frame costs about as much as reading a few
	messages. While
	anything is waiting or stalled, new batches go to `queue` instead; a batch
	that reaches a namespace the client hasn't defined goes to `hold`.
]]
function Serdes.trustedReceiver(
	readers: { [number]: Reader },
	targets: { [number]: { Listener } },
	queue: (batch: buffer, refs: { any }?) -> (),
	hold: (batch: buffer, refs: { any }?, cursor: number, refIndex: number, id: number) -> (),
	shapes: Shapes?
): (batch: any, refs: any) -> ()
	return function(batch: any, refs: any)
		if typeof(batch) ~= "buffer" then
			return
		end
		if trustedStalled or trustedWaiting then
			queue(batch, refs)
			return
		end
		local length = buffer.len(batch)
		rbuff = batch
		rlength = length
		rcursor = 0
		rrefs = refs
		rrefIndex = 0
		rtrusted = true
		rsender = nil
		rreaders = readers
		rtargets = targets
		rshapes = shapes
		dispatchIndex = 0
		trustedStalled = true
		local generation = readerGeneration + 1
		readerGeneration = generation

		-- The loop of readLoop, written out here: one function less per batch
		-- is a measurable part of reading a small one.
		while rcursor < length do
			local start = rcursor
			local id = buffer.readu8(batch, start)
			if id < 128 then
				rcursor = start + 1
			else
				id = (id - 128) * 256 + buffer.readu8(batch, start + 1)
				rcursor = start + 2
			end

			local read = readers[id]
			if read == nil then
				trustedStalled = false
				rrefs = nil
				rshapes = nil
				hold(batch, refs, start, rrefIndex, id)
				return
			end
			local value = read(nil)

			local listeners = targets[id]
			if listeners then
				local count = #listeners
				if count == 1 then
					-- If it yields, this message has nothing left to resume.
					dispatchIndex = -1
					listeners[1](value, nil)
					if readerGeneration ~= generation then
						return
					end
					dispatchIndex = 0
				elseif count > 1 then
					dispatchList = listeners
					dispatchValue = value
					for index = 1, count do
						dispatchIndex = index
						listeners[index](value, nil)
						if readerGeneration ~= generation then
							return
						end
					end
					dispatchIndex = 0
					dispatchList = nil
					dispatchValue = nil
				end
			end
		end
		trustedStalled = false
		rrefs = nil
		rshapes = nil
	end
end

function Serdes.readU8(): number
	local value = buffer.readu8(rbuff, rcursor)
	rcursor += 1
	return value
end

-- Refuses counts that can't possibly fit in what's left of an untrusted batch,
-- so a forged length can't make us allocate or loop without bound. Items that
-- take no bytes come out of the batch's allowance instead (see `process`):
-- one for all of its collections, so repeating one doesn't multiply them.
local function checkCount(count: number, minSize: number)
	if rtrusted then
		return
	end
	if minSize > 0 then
		if count * minSize > buffer.len(rbuff) - rcursor then
			error("count exceeds remaining data", 0)
		end
	else
		rzeroLeft -= count
		if rzeroLeft < 0 then
			error("too many zero-byte items", 0)
		end
	end
end

--------------------------------------------------------------------------------
-- Varints (LEB128): 1 byte below 128, 2 below 16384, ... up to 2^53
--------------------------------------------------------------------------------

-- Caller must have reserved 8 bytes.
local function writeVarUIntRaw(value: number)
	while value >= 128 do
		buffer.writeu8(wbuff, wcursor, value % 128 + 128)
		wcursor += 1
		value //= 128
	end
	buffer.writeu8(wbuff, wcursor, value)
	wcursor += 1
end

local function writeVarUInt(value: number)
	if wcursor + 8 > wsize then
		grow(8)
	end
	writeVarUIntRaw(value)
end

local function readVarUInt(): number
	local byte = buffer.readu8(rbuff, rcursor)
	rcursor += 1
	if byte < 128 then
		return byte
	end

	local value = byte - 128
	local scale = 128
	repeat
		byte = buffer.readu8(rbuff, rcursor)
		rcursor += 1
		value += (byte % 128) * scale
		scale *= 128
		if scale > 2 ^ 56 then
			error("varint too long", 0)
		end
	until byte < 128
	return value
end

-- Call ids.
Serdes.readVarUInt = readVarUInt

--[[
	The reader for message id 0 (CONTROL_ID). `onResync` runs when the other
	side lost track of the key sets this side sends it, with the player who
	sent it (on the server), and should call `resetShapes` on the channel
	going back to them.
]]
function Serdes.controlReader(onResync: (sender: Player?) -> ()): Reader
	return function(sender: Player?): nil
		local code = buffer.readu8(rbuff, rcursor)
		rcursor += 1
		if code == CONTROL_RESET then
			local shapes = rshapes
			if shapes then
				clearShapes(shapes)
			end
		elseif code == CONTROL_RESYNC then
			onResync(sender)
		else
			error("unknown control message", 0)
		end
		return nil
	end
end

-- Array counts: most fit in one byte, so that case skips the call. Caller must
-- have reserved 8 bytes.
local function writeCountRaw(count: number)
	if count < 128 then
		buffer.writeu8(wbuff, wcursor, count)
		wcursor += 1
	else
		writeVarUIntRaw(count)
	end
end

local function readCount(): number
	local byte = buffer.readu8(rbuff, rcursor)
	if byte < 128 then
		rcursor += 1
		return byte
	end
	return readVarUInt()
end

--[[
	Array counts. On a stream that remembers (see "Remembered key sets"), a
	count equal to the same codec's last one goes as the single byte
	SAME_COUNT, so a list of fixed length sent again and again doesn't repeat
	its length. To keep that byte free, a real count of 127 takes two bytes (a
	varint with a zero second byte). `key` identifies the array codec.
]]
local SAME_COUNT = 127

-- Caller must have reserved 8 bytes.
local function writeArrayCount(key: any, count: number)
	local shapes = wshapes
	if shapes then
		local counts = shapes.lastCounts
		if counts[key] == count then
			buffer.writeu8(wbuff, wcursor, SAME_COUNT)
			wcursor += 1
			return
		end
		counts[key] = count
		wtouched = shapes
	else
		-- A reader that remembers counts may still read this one (see passCounts).
		local channel = wchannel :: Channel
		local keys = channel.countKeys
		if keys == nil then
			keys = {}
			channel.countKeys = keys
		end
		keys[key] = true
	end
	if count < SAME_COUNT then
		buffer.writeu8(wbuff, wcursor, count)
		wcursor += 1
	elseif count == SAME_COUNT then
		buffer.writeu8(wbuff, wcursor, SAME_COUNT + 128)
		buffer.writeu8(wbuff, wcursor + 1, 0)
		wcursor += 2
	else
		writeVarUIntRaw(count)
	end
end

local function readArrayCount(key: any): number
	local byte = buffer.readu8(rbuff, rcursor)
	local count
	if byte < SAME_COUNT then
		rcursor += 1
		count = byte
	elseif byte == SAME_COUNT then
		rcursor += 1
		local shapes = rshapes
		local last = if shapes and not shapes.desynced then shapes.lastCounts[key] else nil
		if last == nil then
			error("unknown array count", 0)
		end
		return last
	else
		count = readVarUInt()
	end
	local shapes = rshapes
	if shapes then
		shapes.lastCounts[key] = count
	end
	return count
end

--------------------------------------------------------------------------------
-- Half-precision floats
--------------------------------------------------------------------------------

local function f16Bits(value: number): number
	if value ~= value then
		return 0x7E00
	end

	local sign = 0
	if value < 0 or (value == 0 and 1 / value < 0) then
		sign = 0x8000
		value = -value
	end

	if value >= 65520 then
		return sign + 0x7C00
	elseif value < 6.103515625e-05 then
		-- Subnormal. Rounding up to 1024 lands exactly on the smallest normal.
		return sign + math.floor(value * 16777216 + 0.5)
	end

	local mantissa, exponent = math.frexp(value)
	-- A rounded fraction of 1024 carries into the exponent, which is correct.
	return sign + (exponent + 14) * 1024 + math.floor((mantissa * 2 - 1) * 1024 + 0.5)
end

local function f16Value(bits: number): number
	local sign = if bits >= 0x8000 then -1 else 1
	local exponent = bit32.extract(bits, 10, 5)
	local fraction = bits % 1024

	if exponent == 0 then
		return sign * fraction * 5.9604644775390625e-08
	elseif exponent == 31 then
		return if fraction == 0 then sign * math.huge else 0 / 0
	end
	return sign * math.ldexp(1024 + fraction, exponent - 25)
end

--------------------------------------------------------------------------------
-- Primitive codecs
--------------------------------------------------------------------------------

-- Structs and arrays handle these types inline instead of calling their codec.
local PRIM_U8 = 1
local PRIM_I8 = 2
local PRIM_U16 = 3
local PRIM_I16 = 4
local PRIM_U32 = 5
local PRIM_I32 = 6
local PRIM_F32 = 7
local PRIM_F64 = 8
local PRIM_VEC3 = 9
local PRIM_CFRAME = 10
local PRIM_COUNT = 10

local function fixed(desc: string, size: number, writeRaw: (any) -> (), read: () -> any): Codec
	return {
		kind = "fixed",
		desc = desc,
		size = size,
		minSize = size,
		writeRaw = writeRaw,
		write = function(value: any)
			if wcursor + size > wsize then
				grow(size)
			end
			writeRaw(value)
		end,
		read = read,
	}
end

Serdes.uint8 = fixed("u8", 1, function(value: number)
	buffer.writeu8(wbuff, wcursor, value)
	wcursor += 1
end, function(): number
	local value = buffer.readu8(rbuff, rcursor)
	rcursor += 1
	return value
end)

Serdes.uint16 = fixed("u16", 2, function(value: number)
	buffer.writeu16(wbuff, wcursor, value)
	wcursor += 2
end, function(): number
	local value = buffer.readu16(rbuff, rcursor)
	rcursor += 2
	return value
end)

Serdes.uint32 = fixed("u32", 4, function(value: number)
	buffer.writeu32(wbuff, wcursor, value)
	wcursor += 4
end, function(): number
	local value = buffer.readu32(rbuff, rcursor)
	rcursor += 4
	return value
end)

Serdes.int8 = fixed("i8", 1, function(value: number)
	buffer.writei8(wbuff, wcursor, value)
	wcursor += 1
end, function(): number
	local value = buffer.readi8(rbuff, rcursor)
	rcursor += 1
	return value
end)

Serdes.int16 = fixed("i16", 2, function(value: number)
	buffer.writei16(wbuff, wcursor, value)
	wcursor += 2
end, function(): number
	local value = buffer.readi16(rbuff, rcursor)
	rcursor += 2
	return value
end)

Serdes.int32 = fixed("i32", 4, function(value: number)
	buffer.writei32(wbuff, wcursor, value)
	wcursor += 4
end, function(): number
	local value = buffer.readi32(rbuff, rcursor)
	rcursor += 4
	return value
end)

Serdes.float16 = fixed("f16", 2, function(value: number)
	buffer.writeu16(wbuff, wcursor, f16Bits(value))
	wcursor += 2
end, function(): number
	local value = f16Value(buffer.readu16(rbuff, rcursor))
	rcursor += 2
	return value
end)

Serdes.float32 = fixed("f32", 4, function(value: number)
	buffer.writef32(wbuff, wcursor, value)
	wcursor += 4
end, function(): number
	local value = buffer.readf32(rbuff, rcursor)
	rcursor += 4
	return value
end)

Serdes.float64 = fixed("f64", 8, function(value: number)
	buffer.writef64(wbuff, wcursor, value)
	wcursor += 8
end, function(): number
	local value = buffer.readf64(rbuff, rcursor)
	rcursor += 8
	return value
end)

Serdes.bool = {
	kind = "bool",
	desc = "bool",
	size = 1,
	minSize = 1,
	writeRaw = function(value: boolean)
		buffer.writeu8(wbuff, wcursor, if value then 1 else 0)
		wcursor += 1
	end,
	write = function(value: boolean)
		if wcursor + 1 > wsize then
			grow(1)
		end
		buffer.writeu8(wbuff, wcursor, if value then 1 else 0)
		wcursor += 1
	end,
	read = function(): boolean
		local value = buffer.readu8(rbuff, rcursor) ~= 0
		rcursor += 1
		return value
	end,
} :: Codec

Serdes.varuint = {
	kind = "var",
	desc = "varuint",
	minSize = 1,
	write = writeVarUInt,
	read = readVarUInt,
} :: Codec

-- Zigzag, so small negative numbers stay small too.
Serdes.varint = {
	kind = "var",
	desc = "varint",
	minSize = 1,
	write = function(value: number)
		writeVarUInt(if value >= 0 then value * 2 else -value * 2 - 1)
	end,
	read = function(): number
		local value = readVarUInt()
		return if value % 2 == 0 then value // 2 else -(value + 1) // 2
	end,
} :: Codec

-- Length as a varint, then the bytes. Structs and arrays inline this.
local function writeString(value: string)
	-- # rather than string.len: it errors on a number instead of sending its digits.
	local length = #value
	local cursor = wcursor
	if cursor + length + 8 > wsize then
		grow(length + 8)
	end
	if length < 128 then
		buffer.writeu8(wbuff, cursor, length)
		cursor += 1
	else
		writeVarUIntRaw(length)
		cursor = wcursor
	end
	buffer.writestring(wbuff, cursor, value)
	wcursor = cursor + length
end

local function readString(): string
	local cursor = rcursor
	local length = buffer.readu8(rbuff, cursor)
	if length < 128 then
		cursor += 1
	else
		length = readVarUInt()
		cursor = rcursor
	end
	local value = buffer.readstring(rbuff, cursor, length)
	rcursor = cursor + length
	return value
end

Serdes.string = {
	kind = "string",
	desc = "str",
	minSize = 1,
	write = writeString,
	read = readString,
} :: Codec

Serdes.buff = {
	kind = "var",
	desc = "buff",
	minSize = 1,
	write = function(value: buffer)
		local length = buffer.len(value)
		local needed = length + 8
		if wcursor + needed > wsize then
			grow(needed)
		end
		writeVarUIntRaw(length)
		buffer.copy(wbuff, wcursor, value)
		wcursor += length
	end,
	read = function(): buffer
		local length = readVarUInt()
		if length > buffer.len(rbuff) - rcursor then
			error("buffer length exceeds remaining data", 0)
		end
		local value = buffer.create(length)
		buffer.copy(value, 0, rbuff, rcursor, length)
		rcursor += length
		return value
	end,
} :: Codec

Serdes.vec2 = fixed("vec2", 8, function(value: Vector2)
	buffer.writef32(wbuff, wcursor, value.X)
	buffer.writef32(wbuff, wcursor + 4, value.Y)
	wcursor += 8
end, function(): Vector2
	local value = Vector2.new(buffer.readf32(rbuff, rcursor), buffer.readf32(rbuff, rcursor + 4))
	rcursor += 8
	return value
end)

Serdes.vec3 = fixed("vec3", 12, function(value: Vector3)
	buffer.writef32(wbuff, wcursor, value.X)
	buffer.writef32(wbuff, wcursor + 4, value.Y)
	buffer.writef32(wbuff, wcursor + 8, value.Z)
	wcursor += 12
end, function(): Vector3
	local value = Vector3.new(
		buffer.readf32(rbuff, rcursor),
		buffer.readf32(rbuff, rcursor + 4),
		buffer.readf32(rbuff, rcursor + 8)
	)
	rcursor += 12
	return value
end)

-- Half precision per axis: good for directions, velocities and small offsets.
Serdes.vec3f16 = fixed("vec3f16", 6, function(value: Vector3)
	buffer.writeu16(wbuff, wcursor, f16Bits(value.X))
	buffer.writeu16(wbuff, wcursor + 2, f16Bits(value.Y))
	buffer.writeu16(wbuff, wcursor + 4, f16Bits(value.Z))
	wcursor += 6
end, function(): Vector3
	local value = Vector3.new(
		f16Value(buffer.readu16(rbuff, rcursor)),
		f16Value(buffer.readu16(rbuff, rcursor + 2)),
		f16Value(buffer.readu16(rbuff, rcursor + 4))
	)
	rcursor += 6
	return value
end)

-- 8 bits per channel, rounded (not truncated) and clamped.
Serdes.color3 = fixed("color3", 3, function(value: Color3)
	buffer.writeu8(wbuff, wcursor, math.clamp(math.round(value.R * 255), 0, 255))
	buffer.writeu8(wbuff, wcursor + 1, math.clamp(math.round(value.G * 255), 0, 255))
	buffer.writeu8(wbuff, wcursor + 2, math.clamp(math.round(value.B * 255), 0, 255))
	wcursor += 3
end, function(): Color3
	local value = Color3.fromRGB(
		buffer.readu8(rbuff, rcursor),
		buffer.readu8(rbuff, rcursor + 1),
		buffer.readu8(rbuff, rcursor + 2)
	)
	rcursor += 3
	return value
end)

-- float32 position + float32 axis-angle rotation, 24 bytes at `at`. Reading
-- turns the axis-angle into a quaternion, so the CFrame is built in one call.
local function writeCFrameAt(b: buffer, at: number, value: CFrame)
	local position = value.Position
	local axis, angle = value:ToAxisAngle()
	buffer.writef32(b, at, position.X)
	buffer.writef32(b, at + 4, position.Y)
	buffer.writef32(b, at + 8, position.Z)
	buffer.writef32(b, at + 12, axis.X * angle)
	buffer.writef32(b, at + 16, axis.Y * angle)
	buffer.writef32(b, at + 20, axis.Z * angle)
end

local function readCFrameAt(b: buffer, at: number): CFrame
	local x = buffer.readf32(b, at)
	local y = buffer.readf32(b, at + 4)
	local z = buffer.readf32(b, at + 8)
	local rx = buffer.readf32(b, at + 12)
	local ry = buffer.readf32(b, at + 16)
	local rz = buffer.readf32(b, at + 20)

	local angle = math.sqrt(rx * rx + ry * ry + rz * rz)
	if angle > 0 then
		local half = angle * 0.5
		local scale = math.sin(half) / angle
		return CFrame.new(x, y, z, rx * scale, ry * scale, rz * scale, math.cos(half))
	end
	return CFrame.new(x, y, z)
end

Serdes.cframe = fixed("cframe", 24, function(value: CFrame)
	writeCFrameAt(wbuff, wcursor, value)
	wcursor += 24
end, function(): CFrame
	local value = readCFrameAt(rbuff, rcursor)
	rcursor += 24
	return value
end)

-- float32 position + "smallest three" quaternion at 15 bits per component
-- (about 0.005 degrees of rotation error) in 18 bytes instead of 24.
local function quantize(component: number): number
	return math.clamp(math.round((component * SQRT2 + 1) * 16383.5), 0, 32767)
end

local function dequantize(value: number): number
	return (value / 16383.5 - 1) * INV_SQRT2
end

Serdes.cframeCompact = fixed("cframeCompact", 18, function(value: CFrame)
	local axis, angle = value:ToAxisAngle()
	local half = angle * 0.5
	local s = math.sin(half)
	local qx, qy, qz, qw = axis.X * s, axis.Y * s, axis.Z * s, math.cos(half)
	local ax, ay, az, aw = math.abs(qx), math.abs(qy), math.abs(qz), math.abs(qw)

	local index, largest, a, b, c
	if ax >= ay and ax >= az and ax >= aw then
		index, largest, a, b, c = 0, qx, qy, qz, qw
	elseif ay >= az and ay >= aw then
		index, largest, a, b, c = 1, qy, qx, qz, qw
	elseif az >= aw then
		index, largest, a, b, c = 2, qz, qx, qy, qw
	else
		index, largest, a, b, c = 3, qw, qx, qy, qz
	end
	-- q and -q are the same rotation; keep the dropped component positive.
	if largest < 0 then
		a, b, c = -a, -b, -c
	end

	buffer.writef32(wbuff, wcursor, value.X)
	buffer.writef32(wbuff, wcursor + 4, value.Y)
	buffer.writef32(wbuff, wcursor + 8, value.Z)
	buffer.writeu32(wbuff, wcursor + 12, index * 1073741824 + quantize(a) * 32768 + quantize(b))
	buffer.writeu16(wbuff, wcursor + 16, quantize(c))
	wcursor += 18
end, function(): CFrame
	local x = buffer.readf32(rbuff, rcursor)
	local y = buffer.readf32(rbuff, rcursor + 4)
	local z = buffer.readf32(rbuff, rcursor + 8)
	local packed = buffer.readu32(rbuff, rcursor + 12)
	local c = dequantize(buffer.readu16(rbuff, rcursor + 16) % 32768)
	rcursor += 18

	local index = packed // 1073741824
	local a = dequantize((packed // 32768) % 32768)
	local b = dequantize(packed % 32768)
	local largest = math.sqrt(math.max(0, 1 - a * a - b * b - c * c))

	if index == 0 then
		return CFrame.new(x, y, z, largest, a, b, c)
	elseif index == 1 then
		return CFrame.new(x, y, z, a, largest, b, c)
	elseif index == 2 then
		return CFrame.new(x, y, z, a, b, largest, c)
	end
	return CFrame.new(x, y, z, a, b, c, largest)
end)

-- Instances ride in the refs array, so they take no space in the buffer.
-- nil is stored as `false` to keep the array free of holes.
Serdes.inst = {
	kind = "fixed",
	desc = "inst",
	size = 0,
	minSize = 0,
	writeRaw = function(value: Instance?)
		wrefCount += 1
		wrefs[wrefCount] = if value == nil then false else value
	end,
	write = function(value: Instance?)
		wrefCount += 1
		wrefs[wrefCount] = if value == nil then false else value
	end,
	read = function(): Instance?
		rrefIndex += 1
		local refs = rrefs
		local value = if refs then refs[rrefIndex] else nil
		return if typeof(value) == "Instance" then value else nil
	end,
} :: Codec

-- Any value Roblox itself can send, passed through the refs array. One tag
-- byte tells nil apart from a stored value.
local function writeUnknownRaw(value: any)
	if value == nil then
		buffer.writeu8(wbuff, wcursor, 0)
	else
		buffer.writeu8(wbuff, wcursor, 1)
		wrefCount += 1
		wrefs[wrefCount] = value
	end
	wcursor += 1
end

local function readUnknown(): any
	local tag = buffer.readu8(rbuff, rcursor)
	rcursor += 1
	if tag == 0 then
		return nil
	end
	rrefIndex += 1
	local refs = rrefs
	return if refs then refs[rrefIndex] else nil
end

Serdes.unknown = fixed("unknown", 1, writeUnknownRaw, readUnknown)

Serdes.nothing = fixed("nothing", 0, function() end, function(): nil
	return nil
end)

Serdes.uint8.prim = PRIM_U8
Serdes.int8.prim = PRIM_I8
Serdes.uint16.prim = PRIM_U16
Serdes.int16.prim = PRIM_I16
Serdes.uint32.prim = PRIM_U32
Serdes.int32.prim = PRIM_I32
Serdes.float32.prim = PRIM_F32
Serdes.float64.prim = PRIM_F64
Serdes.vec3.prim = PRIM_VEC3
Serdes.cframe.prim = PRIM_CFRAME

--------------------------------------------------------------------------------
-- Enums
--------------------------------------------------------------------------------

-- One of a fixed list of values, sent as its index (1 byte for up to 256).
function Serdes.enum(values: { any }): Codec
	local count = #values
	if count == 0 or count > 65536 then
		error("[DogNet] enum needs between 1 and 65536 values", 3)
	end

	local lookup = {}
	local descParts = {}
	for index, value in values do
		if lookup[value] ~= nil then
			error(`[DogNet] enum value {value} is listed twice`, 3)
		end
		lookup[value] = index - 1
		table.insert(descParts, tostring(value))
	end
	local desc = `enum<{table.concat(descParts, ",")}>`

	local function indexOf(value: any): number
		local index = lookup[value]
		if index == nil then
			error(`[DogNet] {value} is not one of this enum's values`, 0)
		end
		return index
	end

	if count <= 256 then
		return fixed(desc, 1, function(value: any)
			buffer.writeu8(wbuff, wcursor, indexOf(value))
			wcursor += 1
		end, function(): any
			local value = values[buffer.readu8(rbuff, rcursor) + 1]
			-- `== nil`: false can be one of the values.
			if value == nil and not rtrusted then
				error("invalid enum value", 0)
			end
			rcursor += 1
			return value
		end)
	end
	return fixed(desc, 2, function(value: any)
		buffer.writeu16(wbuff, wcursor, indexOf(value))
		wcursor += 2
	end, function(): any
		local value = values[buffer.readu16(rbuff, rcursor) + 1]
		if value == nil and not rtrusted then
			error("invalid enum value", 0)
		end
		rcursor += 2
		return value
	end)
end

-- A Roblox EnumItem of one Enum, sent as its Value in as few bytes as that Enum allows.
function Serdes.enumItem(enumType: Enum): Codec
	local byValue: { [number]: EnumItem } = {}
	local low, high = 0, 0
	for _, item in enumType:GetEnumItems() do
		byValue[item.Value] = item
		low = math.min(low, item.Value)
		high = math.max(high, item.Value)
	end
	local enumName = string.gsub(tostring(enumType), "^Enum%.", "")
	local desc = `enumItem<{enumName}>`

	local function valueOf(item: EnumItem): number
		if item.EnumType ~= enumType then
			error(`[DogNet] expected an Enum.{enumName} item, got {item}`, 0)
		end
		return item.Value
	end

	if low >= 0 and high < 256 then
		return fixed(desc, 1, function(item: EnumItem)
			buffer.writeu8(wbuff, wcursor, valueOf(item))
			wcursor += 1
		end, function(): EnumItem
			local item = byValue[buffer.readu8(rbuff, rcursor)]
			if item == nil and not rtrusted then
				error("invalid enumItem value", 0)
			end
			rcursor += 1
			return item
		end)
	elseif low >= 0 and high < 65536 then
		return fixed(desc, 2, function(item: EnumItem)
			buffer.writeu16(wbuff, wcursor, valueOf(item))
			wcursor += 2
		end, function(): EnumItem
			local item = byValue[buffer.readu16(rbuff, rcursor)]
			if item == nil and not rtrusted then
				error("invalid enumItem value", 0)
			end
			rcursor += 2
			return item
		end)
	end
	return fixed(desc, 4, function(item: EnumItem)
		buffer.writei32(wbuff, wcursor, valueOf(item))
		wcursor += 4
	end, function(): EnumItem
		local item = byValue[buffer.readi32(rbuff, rcursor)]
		if item == nil and not rtrusted then
			error("invalid enumItem value", 0)
		end
		rcursor += 4
		return item
	end)
end

--------------------------------------------------------------------------------
-- Combinators
--------------------------------------------------------------------------------

local function assertCodec(value: any, context: string)
	if type(value) ~= "table" or type(value.read) ~= "function" or type(value.write) ~= "function" then
		error(`[DogNet] {context} is not a DogNet data type (got {typeof(value)})`, 4)
	end
end

function Serdes.optional(inner: Codec): Codec
	assertCodec(inner, "optional's argument")
	local innerWrite = inner.write
	local innerRead = inner.read

	return {
		kind = "optional",
		desc = `{inner.desc}?`,
		minSize = 1,
		inner = inner,
		write = function(value: any)
			if wcursor + 1 > wsize then
				grow(1)
			end
			if value == nil then
				buffer.writeu8(wbuff, wcursor, 0)
				wcursor += 1
			else
				buffer.writeu8(wbuff, wcursor, 1)
				wcursor += 1
				innerWrite(value)
			end
		end,
		read = function(): any
			local present = buffer.readu8(rbuff, rcursor)
			rcursor += 1
			if present == 0 then
				return nil
			end
			return innerRead()
		end,
	}
end

-- Booleans packed 8 per byte.
local function boolArray(): Codec
	local countKey = {}
	return {
		kind = "var",
		desc = "bool[]",
		minSize = 1,
		write = function(value: { boolean })
			local count = #value
			local bytes = (count + 7) // 8
			local needed = bytes + 8
			if wcursor + needed > wsize then
				grow(needed)
			end
			writeArrayCount(countKey, count)

			local b, at = wbuff, wcursor
			-- 32 per writeu32 (same bytes as 8 per writeu8). Each bit gets its own
			-- local and the sum is a balanced tree: one `a + b + c ...` chain per
			-- word was about 15% slower under native codegen.
			local words = count // 32
			for word = 0, words - 1 do
				local base = word * 32
				local x1 = if value[base + 1] then 1 else 0
				local x2 = if value[base + 2] then 2 else 0
				local x3 = if value[base + 3] then 4 else 0
				local x4 = if value[base + 4] then 8 else 0
				local x5 = if value[base + 5] then 16 else 0
				local x6 = if value[base + 6] then 32 else 0
				local x7 = if value[base + 7] then 64 else 0
				local x8 = if value[base + 8] then 128 else 0
				local x9 = if value[base + 9] then 256 else 0
				local x10 = if value[base + 10] then 512 else 0
				local x11 = if value[base + 11] then 1024 else 0
				local x12 = if value[base + 12] then 2048 else 0
				local x13 = if value[base + 13] then 4096 else 0
				local x14 = if value[base + 14] then 8192 else 0
				local x15 = if value[base + 15] then 16384 else 0
				local x16 = if value[base + 16] then 32768 else 0
				local x17 = if value[base + 17] then 65536 else 0
				local x18 = if value[base + 18] then 131072 else 0
				local x19 = if value[base + 19] then 262144 else 0
				local x20 = if value[base + 20] then 524288 else 0
				local x21 = if value[base + 21] then 1048576 else 0
				local x22 = if value[base + 22] then 2097152 else 0
				local x23 = if value[base + 23] then 4194304 else 0
				local x24 = if value[base + 24] then 8388608 else 0
				local x25 = if value[base + 25] then 16777216 else 0
				local x26 = if value[base + 26] then 33554432 else 0
				local x27 = if value[base + 27] then 67108864 else 0
				local x28 = if value[base + 28] then 134217728 else 0
				local x29 = if value[base + 29] then 268435456 else 0
				local x30 = if value[base + 30] then 536870912 else 0
				local x31 = if value[base + 31] then 1073741824 else 0
				local x32 = if value[base + 32] then 2147483648 else 0
				buffer.writeu32(
					b,
					at + word * 4,
					(
						(((x1 + x2) + (x3 + x4)) + ((x5 + x6) + (x7 + x8)))
						+ (((x9 + x10) + (x11 + x12)) + ((x13 + x14) + (x15 + x16)))
					)
						+ (
							(((x17 + x18) + (x19 + x20)) + ((x21 + x22) + (x23 + x24)))
							+ (((x25 + x26) + (x27 + x28)) + ((x29 + x30) + (x31 + x32)))
						)
				)
			end
			local full = count // 8
			for byteIndex = words * 4, full - 1 do
				local base = byteIndex * 8
				buffer.writeu8(
					b,
					at + byteIndex,
					(if value[base + 1] then 1 else 0)
						+ (if value[base + 2] then 2 else 0)
						+ (if value[base + 3] then 4 else 0)
						+ (if value[base + 4] then 8 else 0)
						+ (if value[base + 5] then 16 else 0)
						+ (if value[base + 6] then 32 else 0)
						+ (if value[base + 7] then 64 else 0)
						+ (if value[base + 8] then 128 else 0)
				)
			end
			if full < bytes then
				local byte = 0
				local bit = 1
				for index = full * 8 + 1, count do
					if value[index] then
						byte += bit
					end
					bit *= 2
				end
				buffer.writeu8(b, at + full, byte)
			end
			wcursor = at + bytes
		end,
		read = function(): { boolean }
			local count = readArrayCount(countKey)
			local bytes = (count + 7) // 8
			local b, at = rbuff, rcursor
			if bytes > buffer.len(b) - at then
				error("count exceeds remaining data", 0)
			end

			local result = table.create(count) :: { boolean }
			-- 32 per readu32, all 32 computed before any store (about 15% faster
			-- than a store after each btest).
			local words = count // 32
			for word = 0, words - 1 do
				local bits = buffer.readu32(b, at + word * 4)
				local base = word * 32
				local x1 = bit32.btest(bits, 1)
				local x2 = bit32.btest(bits, 2)
				local x3 = bit32.btest(bits, 4)
				local x4 = bit32.btest(bits, 8)
				local x5 = bit32.btest(bits, 16)
				local x6 = bit32.btest(bits, 32)
				local x7 = bit32.btest(bits, 64)
				local x8 = bit32.btest(bits, 128)
				local x9 = bit32.btest(bits, 256)
				local x10 = bit32.btest(bits, 512)
				local x11 = bit32.btest(bits, 1024)
				local x12 = bit32.btest(bits, 2048)
				local x13 = bit32.btest(bits, 4096)
				local x14 = bit32.btest(bits, 8192)
				local x15 = bit32.btest(bits, 16384)
				local x16 = bit32.btest(bits, 32768)
				local x17 = bit32.btest(bits, 65536)
				local x18 = bit32.btest(bits, 131072)
				local x19 = bit32.btest(bits, 262144)
				local x20 = bit32.btest(bits, 524288)
				local x21 = bit32.btest(bits, 1048576)
				local x22 = bit32.btest(bits, 2097152)
				local x23 = bit32.btest(bits, 4194304)
				local x24 = bit32.btest(bits, 8388608)
				local x25 = bit32.btest(bits, 16777216)
				local x26 = bit32.btest(bits, 33554432)
				local x27 = bit32.btest(bits, 67108864)
				local x28 = bit32.btest(bits, 134217728)
				local x29 = bit32.btest(bits, 268435456)
				local x30 = bit32.btest(bits, 536870912)
				local x31 = bit32.btest(bits, 1073741824)
				local x32 = bit32.btest(bits, 2147483648)
				result[base + 1] = x1
				result[base + 2] = x2
				result[base + 3] = x3
				result[base + 4] = x4
				result[base + 5] = x5
				result[base + 6] = x6
				result[base + 7] = x7
				result[base + 8] = x8
				result[base + 9] = x9
				result[base + 10] = x10
				result[base + 11] = x11
				result[base + 12] = x12
				result[base + 13] = x13
				result[base + 14] = x14
				result[base + 15] = x15
				result[base + 16] = x16
				result[base + 17] = x17
				result[base + 18] = x18
				result[base + 19] = x19
				result[base + 20] = x20
				result[base + 21] = x21
				result[base + 22] = x22
				result[base + 23] = x23
				result[base + 24] = x24
				result[base + 25] = x25
				result[base + 26] = x26
				result[base + 27] = x27
				result[base + 28] = x28
				result[base + 29] = x29
				result[base + 30] = x30
				result[base + 31] = x31
				result[base + 32] = x32
			end
			local full = count // 8
			for byteIndex = words * 4, full - 1 do
				local byte = buffer.readu8(b, at + byteIndex)
				local base = byteIndex * 8
				result[base + 1] = bit32.btest(byte, 1)
				result[base + 2] = bit32.btest(byte, 2)
				result[base + 3] = bit32.btest(byte, 4)
				result[base + 4] = bit32.btest(byte, 8)
				result[base + 5] = bit32.btest(byte, 16)
				result[base + 6] = bit32.btest(byte, 32)
				result[base + 7] = bit32.btest(byte, 64)
				result[base + 8] = bit32.btest(byte, 128)
			end
			if full < bytes then
				local byte = buffer.readu8(b, at + full)
				local bit = 1
				for index = full * 8 + 1, count do
					result[index] = bit32.btest(byte, bit)
					bit *= 2
				end
			end
			rcursor = at + bytes
			return result
		end,
	}
end

-- Arrays of numbers and vectors, one tight loop each way.
local function primArray(item: Codec, prim: number): Codec
	local countKey = {}
	local size = item.size :: number
	local isBytes = prim == PRIM_U8
	local write, read

	-- 8- and 16-bit items are packed into one writeu32 per 4 or 2 of them,
	-- two writes per step, with the items loaded into locals first (about a
	-- third less time than a write per item). Masking each item keeps what writeu8
	-- and friends do with out-of-range values: keep the low bits of the
	-- integer part.
	local packBytes = function(list: { any }, count: number, b: buffer, at: number)
		local band = bit32.band
		local full = count // 8
		for step = 0, full - 1 do
			local index = step * 8
			local x1, x2, x3, x4 = list[index + 1], list[index + 2], list[index + 3], list[index + 4]
			local x5, x6, x7, x8 = list[index + 5], list[index + 6], list[index + 7], list[index + 8]
			buffer.writeu32(b, at + index, band(x1, 255) + band(x2, 255) * 256 + band(x3, 255) * 65536 + band(x4, 255) * 16777216)
			buffer.writeu32(b, at + index + 4, band(x5, 255) + band(x6, 255) * 256 + band(x7, 255) * 65536 + band(x8, 255) * 16777216)
		end
		for index = full * 8 + 1, count do
			buffer.writeu8(b, at + index - 1, band(list[index], 255))
		end
	end
	local packShorts = function(list: { any }, count: number, b: buffer, at: number)
		local band = bit32.band
		local full = count // 4
		for step = 0, full - 1 do
			local index = step * 4
			local x1, x2, x3, x4 = list[index + 1], list[index + 2], list[index + 3], list[index + 4]
			buffer.writeu32(b, at + index * 2, band(x1, 65535) + band(x2, 65535) * 65536)
			buffer.writeu32(b, at + index * 2 + 4, band(x3, 65535) + band(x4, 65535) * 65536)
		end
		for index = full * 4 + 1, count do
			buffer.writeu16(b, at + (index - 1) * 2, band(list[index], 65535))
		end
	end

	if prim == PRIM_U8 then
		write = packBytes
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = buffer.readu8(b, at + index - 1)
			end
		end
	elseif prim == PRIM_I8 then
		write = packBytes
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = buffer.readi8(b, at + index - 1)
			end
		end
	elseif prim == PRIM_U16 then
		write = packShorts
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = buffer.readu16(b, at + (index - 1) * 2)
			end
		end
	elseif prim == PRIM_I16 then
		write = packShorts
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = buffer.readi16(b, at + (index - 1) * 2)
			end
		end
	elseif prim == PRIM_U32 then
		write = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				buffer.writeu32(b, at + (index - 1) * 4, list[index])
			end
		end
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = buffer.readu32(b, at + (index - 1) * 4)
			end
		end
	elseif prim == PRIM_I32 then
		write = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				buffer.writei32(b, at + (index - 1) * 4, list[index])
			end
		end
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = buffer.readi32(b, at + (index - 1) * 4)
			end
		end
	elseif prim == PRIM_F32 then
		write = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				buffer.writef32(b, at + (index - 1) * 4, list[index])
			end
		end
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = buffer.readf32(b, at + (index - 1) * 4)
			end
		end
	elseif prim == PRIM_F64 then
		write = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				buffer.writef64(b, at + (index - 1) * 8, list[index])
			end
		end
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = buffer.readf64(b, at + (index - 1) * 8)
			end
		end
	elseif prim == PRIM_VEC3 then
		write = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				local vector = list[index]
				local offset = at + (index - 1) * 12
				buffer.writef32(b, offset, vector.X)
				buffer.writef32(b, offset + 4, vector.Y)
				buffer.writef32(b, offset + 8, vector.Z)
			end
		end
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				local offset = at + (index - 1) * 12
				list[index] =
					Vector3.new(buffer.readf32(b, offset), buffer.readf32(b, offset + 4), buffer.readf32(b, offset + 8))
			end
		end
	else
		write = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				writeCFrameAt(b, at + (index - 1) * 24, list[index])
			end
		end
		read = function(list: { any }, count: number, b: buffer, at: number)
			for index = 1, count do
				list[index] = readCFrameAt(b, at + (index - 1) * 24)
			end
		end
	end

	return {
		kind = "var",
		desc = `{item.desc}[]`,
		minSize = 1,
		write = function(list: { any })
			local count = #list
			local needed = count * size + 8
			if wcursor + needed > wsize then
				grow(needed)
			end
			writeArrayCount(countKey, count)
			local at = wcursor
			write(list, count, wbuff, at)
			wcursor = at + count * size
		end,
		read = function(): { any }
			local count = readArrayCount(countKey)
			if not rtrusted then
				checkCount(count, size)
			end
			local b, at = rbuff, rcursor
			if count * size > buffer.len(b) - at then
				error("count exceeds remaining data", 0)
			end
			local list
			if isBytes then
				-- Four bytes per read. Measured in a game against building the
				-- table from string.byte on a readstring of the bytes: about 8%
				-- less server time per message, since the throwaway string cost
				-- the collector more than the single call saved.
				list = table.create(count)
				local full = count // 4
				for word = 0, full - 1 do
					local value = buffer.readu32(b, at + word * 4)
					local index = word * 4
					list[index + 1] = bit32.band(value, 255)
					list[index + 2] = bit32.extract(value, 8, 8)
					list[index + 3] = bit32.extract(value, 16, 8)
					list[index + 4] = bit32.rshift(value, 24)
				end
				for index = full * 4 + 1, count do
					list[index] = buffer.readu8(b, at + index - 1)
				end
			else
				list = table.create(count)
				read(list, count, b, at)
			end
			rcursor = at + count * size
			return list
		end,
	}
end

--[[
	String arrays. After the count, one of two layouts:
		[each string's length] [all their bytes, back to back]
		[0x80 0x00] [runs] [each run's length and repeats] [each run's bytes, once]
	Lengths go first: after Roblox's compression that comes out a little
	smaller than the bytes first (a sorted list's lengths fold away). The
	bytes go in with one table.concat and one writestring, about twice as
	fast as a writestring per string. The second layout, marked by a varint
	no writer makes (0 in two bytes), is for lists where strings often repeat
	the one before: each run of equal strings goes once, with its repeats.
]]

-- Rewrites the array from `at` as runs of equal strings.
local function writeStringRuns(list: { string }, count: number, at: number)
	local runs = 0
	local bytes = 0
	local previous: string? = nil
	for index = 1, count do
		local value = list[index]
		if value ~= previous then
			runs += 1
			bytes += string.len(value)
			previous = value
		end
	end
	wcursor = at
	-- Two varints of up to 8 bytes per run.
	local needed = 16 + runs * 16 + bytes
	if at + needed > wsize then
		grow(needed)
	end
	buffer.writeu8(wbuff, at, 0x80)
	buffer.writeu8(wbuff, at + 1, 0)
	wcursor = at + 2
	writeVarUIntRaw(runs)
	local unique = table.create(runs)
	local run = 0
	local index = 1
	while index <= count do
		local value = list[index]
		local last = index
		while last < count and list[last + 1] == value do
			last += 1
		end
		run += 1
		unique[run] = value
		writeCountRaw(string.len(value))
		writeCountRaw(last - index)
		index = last + 1
	end
	local blob = table.concat(unique, "", 1, runs)
	buffer.writestring(wbuff, wcursor, blob)
	wcursor += string.len(blob)
end

--[[
	Reads `count` strings laid out as [each one's length] [all their bytes]
	at the read cursor into list[1..count], and returns their total bytes.
	Maps with a lengths layout read their keys with this too.
]]
local function readStrings(list: { string }, count: number): number
	local b = rbuff
	local lengthsAt = rcursor
	local length = buffer.len(b)
	-- Every string takes at least its length byte.
	if count > length - lengthsAt then
		error("count exceeds remaining data", 0)
	end
	-- Where the bytes start depends on the lengths: one byte each, four
	-- checked per read, unless one is 128 or more.
	local total = 0
	local short = true
	local full = count // 4
	for word = 0, full - 1 do
		local value = buffer.readu32(b, lengthsAt + word * 4)
		if bit32.btest(value, 0x80808080) then
			short = false
			break
		end
		total += bit32.band(value, 255) + bit32.extract(value, 8, 8)
		total += bit32.extract(value, 16, 8) + bit32.rshift(value, 24)
	end
	if short then
		for index = full * 4, count - 1 do
			local byte = buffer.readu8(b, lengthsAt + index)
			if byte >= 128 then
				short = false
				break
			end
			total += byte
		end
	end
	local bytesAt
	if short then
		bytesAt = lengthsAt + count
	else
		total = 0
		for _ = 1, count do
			total += readVarUInt()
		end
		bytesAt = rcursor
	end
	if total > length - bytesAt then
		error("malformed string array", 0)
	end
	-- Cut from one string of all the bytes: string.sub is a fastcall builtin
	-- where readstring is a full C call, which outweighs the extra string (8
	-- to 18% less time on all-different names).
	local bytes = buffer.readstring(b, bytesAt, total)
	local from = 1
	if short then
		for index = 1, count do
			local to = from + buffer.readu8(b, lengthsAt + index - 1)
			list[index] = string.sub(bytes, from, to - 1)
			from = to
		end
	else
		rcursor = lengthsAt
		for index = 1, count do
			local to = from + readVarUInt()
			list[index] = string.sub(bytes, from, to - 1)
			from = to
		end
	end
	rcursor = bytesAt + total
	return total
end

local function readStringRuns(count: number): { string }
	local runs = readVarUInt()
	local b = rbuff
	local length = buffer.len(b)
	-- Every run takes at least two bytes, and repeats are capped like zero-size items.
	if runs > count or runs * 2 > length - rcursor or count - runs > MAX_ZERO_SIZE_COUNT then
		error("malformed string array", 0)
	end
	-- Repeats take no bytes, so like zero-byte items they come out of the
	-- batch's allowance: sending the array again doesn't make them free.
	if not rtrusted then
		rzeroLeft -= count - runs
		if rzeroLeft < 0 then
			error("too many repeated strings", 0)
		end
	end
	local runLengths = table.create(runs)
	local runRepeats = table.create(runs)
	local total = 0
	local items = 0
	for run = 1, runs do
		local runLength = readVarUInt()
		local repeats = readVarUInt()
		runLengths[run] = runLength
		runRepeats[run] = repeats
		total += runLength
		items += 1 + repeats
	end
	local at = rcursor
	if items ~= count or total > length - at then
		error("malformed string array", 0)
	end
	local bytes = buffer.readstring(b, at, total)
	rcursor = at + total
	if runs == 1 then
		return table.create(count, bytes)
	end
	local list = table.create(count)
	local index = 0
	local from = 1
	for run = 1, runs do
		local to = from + runLengths[run]
		local value = string.sub(bytes, from, to - 1)
		from = to
		for _ = 0, runRepeats[run] do
			index += 1
			list[index] = value
		end
	end
	return list
end

local function stringArray(): Codec
	local countKey = {}
	return {
		kind = "var",
		desc = "str[]",
		minSize = 2,
		write = function(list: { string })
			local count = #list
			local needed = 16 + count
			if wcursor + needed > wsize then
				grow(needed)
			end
			writeArrayCount(countKey, count)
			local b, at = wbuff, wcursor
			-- One byte per length, assuming they're all below 128, and a
			-- rewrite if not. Under native codegen string.len is a load where
			-- # on a value of unknown type is a call, and math.max keeps the
			-- loop free of branches (about 30% faster than a branch and #).
			-- Repeats of the string before are counted on the way.
			local longest = 0
			local repeats = 0
			local repeated = 0
			local previous: any = nil
			for index = 1, count do
				local value = list[index]
				-- table.concat would take numbers too.
				if type(value) ~= "string" then
					error(`string arrays can only hold strings, got {typeof(value)}`, 0)
				end
				local length = string.len(value)
				longest = math.max(longest, length)
				if value == previous then
					repeats += 1
					repeated += length
				end
				previous = value
				buffer.writeu8(b, at + index - 1, length)
			end
			-- Runs pay off once the repeats' bytes outweigh a repeat count per run.
			if repeats > 0 and repeats <= MAX_ZERO_SIZE_COUNT and repeats + repeated > count - repeats + 4 then
				writeStringRuns(list, count, at)
				return
			end
			local blob = table.concat(list, "", 1, count)
			local total = string.len(blob)
			local bytesAt = at + count
			if longest >= 128 then
				wcursor = at
				if at + count * 8 > wsize then
					grow(count * 8)
				end
				for index = 1, count do
					writeVarUIntRaw(string.len(list[index]))
				end
				bytesAt = wcursor
			end
			wcursor = bytesAt
			if bytesAt + total > wsize then
				grow(total)
			end
			buffer.writestring(wbuff, bytesAt, blob)
			wcursor = bytesAt + total
		end,
		read = function(): { string }
			local count = readArrayCount(countKey)
			if count == 0 then
				return {}
			end
			local b, at = rbuff, rcursor
			if buffer.readu8(b, at) == 0x80 and buffer.readu8(b, at + 1) == 0 then
				rcursor = at + 2
				return readStringRuns(count)
			end
			if count > buffer.len(b) - at then
				error("count exceeds remaining data", 0)
			end
			local list = table.create(count)
			readStrings(list, count)
			return list
		end,
	} :: Codec
end

function Serdes.array(item: Codec): Codec
	assertCodec(item, "array's argument")
	if item.kind == "bool" then
		return boolArray()
	elseif item == Serdes.string then
		return stringArray()
	elseif item.prim then
		return primArray(item, item.prim :: number)
	end

	local countKey = {}
	local itemSize = item.size
	local itemWrite = item.write
	local itemWriteRaw = item.writeRaw
	local itemRead = item.read
	local itemMinSize = item.minSize
	local writeMany = item.writeMany
	local readMany = item.readMany

	local write
	if itemSize and writeMany then
		-- Fixed-size structs: reserve the whole array once, then write the items inline.
		write = function(list: { any })
			local count = #list
			local needed = count * itemSize + 8
			if wcursor + needed > wsize then
				grow(needed)
			end
			writeArrayCount(countKey, count)
			writeMany(list, count)
		end
	elseif itemSize and itemWriteRaw then
		-- Other fixed-size items: reserve once, then write unchecked.
		write = function(list: { any })
			local count = #list
			local needed = count * itemSize + 8
			if wcursor + needed > wsize then
				grow(needed)
			end
			writeArrayCount(countKey, count)
			for index = 1, count do
				itemWriteRaw(list[index])
			end
		end
	else
		write = function(list: { any })
			local count = #list
			if wcursor + 8 > wsize then
				grow(8)
			end
			writeArrayCount(countKey, count)
			for index = 1, count do
				itemWrite(list[index])
			end
		end
	end

	local read
	if readMany then
		read = function(): { any }
			local count = readArrayCount(countKey)
			if not rtrusted then
				checkCount(count, itemMinSize)
			end
			local list = table.create(count)
			readMany(list, count)
			return list
		end
	else
		read = function(): { any }
			local count = readArrayCount(countKey)
			if not rtrusted then
				checkCount(count, itemMinSize)
			end
			local list = table.create(count)
			for index = 1, count do
				list[index] = itemRead()
			end
			return list
		end
	end

	return {
		kind = "var",
		desc = `{item.desc}[]`,
		minSize = 1,
		write = write,
		read = read,
	}
end

--[[
	Maps with string keys, the usual kind, get their own layout. A header
	varint says which:
		0      the empty map
		1, 2   [key bytes] [the keys joined by "\0"] [values]
		3, 4   [count] [each key's length] [the keys' bytes] [values]
		5 + s  [values], with the keys of the key set remembered in slot s
	Values come in the keys' order. 2 and 4 also add the key set to the ones
	the stream remembers (see "Remembered key sets"), so the next map with
	those keys (the same table, or one built the same way, sent again with new
	values) goes as a one-byte reference and its values.

	Of the two ways to send keys, Roblox's compression shrinks joined keys
	more when they share a start ("Key12", "Key13") or vary in length, and
	lengths more when they're all about as long with nothing in common (user
	ids, GUIDs): the separators would cost bits there, and equal lengths fold
	away. That choice was the smaller one on every key set measured.

	Writing, the map's first key finds the remembered key sets it could have,
	and the map is checked against one as its values go in. Reading, a
	reference gives the keys, and the result is a clone of a table holding
	exactly those keys, so filling it in only updates slots: no key strings
	to build, no inserts, no rehashing. After a few maps in a row whose keys
	weren't remembered, a codec stops adding key sets for a while (see
	MAP_COOLDOWN), so maps whose keys never repeat don't pay for it.
]]
local MAP_EMPTY = 0
local MAP_JOINED = 1
local MAP_JOINED_ADD = 2
local MAP_LENGTHS = 3
local MAP_LENGTHS_ADD = 4
local MAP_SHAPE = 5

-- Where map writers put u8 values while going through the keys. Shared: a
-- writer is done with it before it writes any values.
local valueScratch = buffer.create(1024)
local scratchSize = 1024

local function growScratch(needed: number): (buffer, number)
	local size = scratchSize
	while size < needed do
		size *= 2
	end
	local values = buffer.create(size)
	buffer.copy(values, 0, valueScratch, 0, scratchSize)
	valueScratch, scratchSize = values, size
	return values, size
end

-- Misses in a row after which a codec stops adding key sets for a while.
local MAP_MISSES_BEFORE_COOLDOWN = 8
-- Misses a codec then writes without adding, doubling with each cooldown up
-- to the max. Hits don't end it: they're from key sets already remembered,
-- which say nothing about the new ones. So maps whose keys never repeat, or
-- that take turns among more key sets than fit (each pushed out before it
-- comes round again), soon pay next to nothing for being watched, while the
-- key sets already remembered keep being used.
local MAP_COOLDOWN = 32
local MAP_COOLDOWN_MAX = 1024
-- The first keys of a codec's last few misses. A key set missed again within
-- them is repeating soon enough to be worth adding, cooldown or not.
local MAP_RECENT_MISSES = 8

-- Cuts keys joined by "\0" (`joined`, read from the batch at `at`) into
-- list[1..count] and returns the count. The separators are found four bytes
-- per read, looking at single bytes only in words that have a zero, and the
-- keys cut with string.sub: string.split took about 30% longer for 500 keys.
local function splitJoined(b: buffer, at: number, joined: string, list: { string }): number
	local length = string.len(joined)
	local stop = at + length
	local count, from, offset, p = 0, 1, 1 - at, at
	while p + 4 <= stop do
		local word = buffer.readu32(b, p)
		if bit32.band(word - 0x01010101, bit32.bnot(word), 0x80808080) ~= 0 then
			for q = p, p + 3 do
				if buffer.readu8(b, q) == 0 then
					count += 1
					list[count] = string.sub(joined, from, q + offset - 1)
					from = q + offset + 1
				end
			end
		end
		p += 4
	end
	for q = p, stop - 1 do
		if buffer.readu8(b, q) == 0 then
			count += 1
			list[count] = string.sub(joined, from, q + offset - 1)
			from = q + offset + 1
		end
	end
	count += 1
	list[count] = string.sub(joined, from, length)
	return count
end

-- Whether keys (in iteration order) mostly start the same way: two of the
-- first three neighbours share their first two bytes.
local function sharePrefix(keys: { string }, count: number): boolean
	local shared = 0
	local a1, a2 = string.byte(keys[1], 1, 2)
	for index = 2, math.min(count, 4) do
		local b1, b2 = string.byte(keys[index], 1, 2)
		if b2 ~= nil and a1 == b1 and a2 == b2 then
			shared += 1
		end
		a1, a2 = b1, b2
	end
	return shared >= math.min(2, count - 1)
end

-- A map's reader for values that take no bytes (Instances): each comes out
-- of the batch's allowance, since a map sent as a remembered key set carries
-- no keys either and would otherwise cost nothing. Kept out of the map's own
-- reader, which stays as small as it is for native code.
local function chargedRead(read: () -> any): () -> any
	return function(): any
		if not rtrusted then
			rzeroLeft -= 1
			if rzeroLeft < 0 then
				error("too many zero-byte items", 0)
			end
		end
		return read()
	end
end

local function stringKeyMap(value: Codec): Codec
	local valueSize = value.size
	local valueWriteRaw = value.writeRaw
	local valueWrite = value.write
	local valueRead = if value.minSize == 0 then chargedRead(value.read) else value.read
	local valueMinSize = value.minSize
	local isU8 = value.prim == PRIM_U8
	-- Numbers and booleans: nothing to keep alive beyond their slot.
	local valuePrim = value.prim
	local plainValues = value.kind == "bool" or (valuePrim ~= nil and valuePrim <= PRIM_F64)
	-- Values that can't add key sets themselves, so writing them can be undone.
	local fixedValues = valueSize ~= nil and valueWriteRaw ~= nil

	local wMisses = 0
	local wCooldown = 0
	local wNextCooldown = MAP_COOLDOWN
	local wRecent: { any } = table.create(MAP_RECENT_MISSES, false)
	local wRecentAt = 0
	-- Reader: an empty table sized for the last map read with its keys, so
	-- the next one doesn't rehash as it fills.
	local rSized: { [string]: any } = {}
	local rSizedCount = 0
	-- Keys read for a map whose key set isn't being added.
	local scratchReadKeys: { string } = {}

	local function resize(result: { [string]: any }, count: number)
		-- If it outgrew the last or is far smaller.
		if count > rSizedCount or count < rSizedCount // 4 then
			rSized = table.clone(result)
			table.clear(rSized)
			rSizedCount = count
		end
	end

	-- Joined keys with u8 values, not added: the usual map whose keys don't
	-- repeat. Each key goes into the result with its value as its separator
	-- turns up (splitJoined without the list: 7% less time for 500 keys).
	local function readJoinedU8(b: buffer, at: number, joined: string): { [string]: any }
		local length = string.len(joined)
		local valuesAt = at + length
		local result: { [string]: any } = table.clone(rSized)
		local count, from, offset, p = 0, 1, 1 - at, at
		while p + 4 <= valuesAt do
			local word = buffer.readu32(b, p)
			if bit32.band(word - 0x01010101, bit32.bnot(word), 0x80808080) ~= 0 then
				for q = p, p + 3 do
					if buffer.readu8(b, q) == 0 then
						count += 1
						result[string.sub(joined, from, q + offset - 1)] = buffer.readu8(b, valuesAt + count - 1)
						from = q + offset + 1
					end
				end
			end
			p += 4
		end
		for q = p, valuesAt - 1 do
			if buffer.readu8(b, q) == 0 then
				count += 1
				result[string.sub(joined, from, q + offset - 1)] = buffer.readu8(b, valuesAt + count - 1)
				from = q + offset + 1
			end
		end
		count += 1
		result[string.sub(joined, from, length)] = buffer.readu8(b, valuesAt + count - 1)
		rcursor = valuesAt + count
		resize(result, count)
		return result
	end

	-- Writes the values of `map` in the order of `keys`, starting at the
	-- cursor. Returns false if the map doesn't iterate to exactly `keys`.
	local function writeValuesChecked(map: { [string]: any }, keys: { string }, count: number, b: buffer): boolean
		local index = 0
		if isU8 then
			local at = wcursor
			for k, v in map do
				index += 1
				if keys[index] ~= k then
					return false
				end
				buffer.writeu8(b, at + index - 1, v)
			end
			wcursor = at + index
		else
			for k, v in map do
				index += 1
				if keys[index] ~= k then
					return false
				end
				(valueWriteRaw :: (any) -> ())(v)
			end
		end
		return index == count
	end

	local function sameKeys(map: { [string]: any }, keys: { string }, count: number): boolean
		local index = 0
		for k in map do
			index += 1
			if keys[index] ~= k then
				return false
			end
		end
		return index == count
	end

	-- Writes `map` with the remembered key set at `index`. Returns false,
	-- having written nothing, if the map doesn't iterate to its keys.
	local function writeShape(map: { [string]: any }, shapes: Shapes, index: number): boolean
		local keys = shapes.keys[index] :: { string }
		local count = shapes.counts[index]
		local start, startRefs = wcursor, wrefCount
		local needed = 8 + (if fixedValues then count * (valueSize :: number) else 0)
		if start + needed > wsize then
			grow(needed)
		end
		writeCountRaw(MAP_SHAPE + index - 1)
		if fixedValues then
			if writeValuesChecked(map, keys, count, wbuff) then
				return true
			end
			-- Instances among the values went into the refs.
			wcursor, wrefCount = start, startRefs
			return false
		end
		-- Other values may be maps that add key sets, which can't be undone:
		-- check the keys before writing any.
		if not sameKeys(map, keys, count) then
			wcursor = start
			return false
		end
		for _, v in map do
			valueWrite(v)
		end
		return true
	end

	-- Keys of the map being written, reused from call to call; copied out
	-- when they become a remembered key set.
	local scratchKeys: { string } = {}

	-- Writes `map` with its keys, adding them to `shapes` if `add`.
	local function writeKeys(map: { [string]: any }, shapes: Shapes?, add: boolean)
		local keys = scratchKeys
		local count = 0
		local vb, size = valueScratch, scratchSize
		local longest, shortest, keyBytes = 0, math.huge, 0
		if isU8 then
			for k, v in map do
				if type(k) ~= "string" then
					error(`map keys must be strings, got {typeof(k)}`, 0)
				end
				count += 1
				keys[count] = k
				local length = string.len(k)
				longest = math.max(longest, length)
				shortest = math.min(shortest, length)
				keyBytes += length
				if count > size then
					vb, size = growScratch(count)
				end
				buffer.writeu8(vb, count - 1, v)
			end
		else
			for k in map do
				if type(k) ~= "string" then
					error(`map keys must be strings, got {typeof(k)}`, 0)
				end
				count += 1
				keys[count] = k
				local length = string.len(k)
				longest = math.max(longest, length)
				shortest = math.min(shortest, length)
				keyBytes += length
			end
		end

		local plain = table.concat(keys, "", 1, count)
		local joined: string? = nil
		if
			count >= 2
			and (longest - shortest >= 2 or sharePrefix(keys, count))
			and not string.find(plain, "\0", 1, true)
		then
			joined = table.concat(keys, "\0", 1, count)
		end
		local shapeSize = keyBytes + SHAPE_KEY_COST * count
		if add and (shapes == nil or shapeSize > shapes.memory) then
			add = false
		end

		local header = if joined
			then (if add then MAP_JOINED_ADD else MAP_JOINED)
			else (if add then MAP_LENGTHS_ADD else MAP_LENGTHS)
		local needed = 32 + keyBytes + count * 9 + (if fixedValues then count * (valueSize :: number) else 0)
		if wcursor + needed > wsize then
			grow(needed)
		end
		writeCountRaw(header)
		if joined then
			local joinedLength = string.len(joined)
			writeCountRaw(joinedLength)
			buffer.writestring(wbuff, wcursor, joined)
			wcursor += joinedLength
		else
			writeCountRaw(count)
			if longest < 128 then
				local b, at = wbuff, wcursor
				for index = 1, count do
					buffer.writeu8(b, at + index - 1, string.len(keys[index]))
				end
				wcursor = at + count
			else
				for index = 1, count do
					writeVarUIntRaw(string.len(keys[index]))
				end
			end
			buffer.writestring(wbuff, wcursor, plain)
			wcursor += keyBytes
		end

		-- Added before the values, as the reader does: they may be maps that add their own.
		if add then
			local owned = shapes :: Shapes
			local list = table.move(keys, 1, count, 1, table.create(count))
			local index = addShape(owned, list, count, shapeSize)
			local first = list[1]
			local byFirst = owned.byFirst[first]
			if byFirst then
				table.insert(byFirst, index)
			else
				owned.byFirst[first] = { index }
			end
			wtouched = owned
		end

		if isU8 then
			buffer.copy(wbuff, wcursor, vb, 0, count)
			wcursor += count
		elseif fixedValues then
			for _, v in map do
				(valueWriteRaw :: (any) -> ())(v)
			end
		else
			for _, v in map do
				valueWrite(v)
			end
		end
	end

	local function writeMap(map: { [string]: any }): ()
		local first = next(map)
		if first == nil then
			if wcursor + 1 > wsize then
				grow(1)
			end
			buffer.writeu8(wbuff, wcursor, MAP_EMPTY)
			wcursor += 1
			return
		end
		local shapes = wshapes
		if shapes == nil then
			writeKeys(map, nil, false)
			return
		end
		local candidates = shapes.byFirst[first]
		if candidates then
			for position = #candidates, 1, -1 do
				if writeShape(map, shapes, candidates[position]) then
					wMisses = 0
					return
				end
			end
		end
		-- Keys not remembered. Whether they get added:
		local add = true
		if table.find(wRecent, first) then
			wMisses = 0
		elseif wCooldown > 0 then
			wCooldown -= 1
			add = false
		else
			wMisses += 1
			if wMisses >= MAP_MISSES_BEFORE_COOLDOWN then
				wMisses = 0
				wCooldown = wNextCooldown
				wNextCooldown = math.min(wNextCooldown * 2, MAP_COOLDOWN_MAX)
				add = false
			end
		end
		wRecentAt = wRecentAt % MAP_RECENT_MISSES + 1
		wRecent[wRecentAt] = first
		writeKeys(map, shapes, add)
	end

	local function readValues(result: { [string]: any }, keys: { string }, count: number)
		if isU8 then
			local b, at = rbuff, rcursor
			if count > buffer.len(b) - at then
				error("count exceeds remaining data", 0)
			end
			for index = 1, count do
				result[keys[index]] = buffer.readu8(b, at + index - 1)
			end
			rcursor = at + count
		else
			if not rtrusted and count * valueMinSize > buffer.len(rbuff) - rcursor then
				error("count exceeds remaining data", 0)
			end
			for index = 1, count do
				result[keys[index]] = valueRead()
			end
		end
	end

	local function readMap(): any
		local header = readCount()
		if header >= MAP_SHAPE then
			local shapes = rshapes
			local index = header - MAP_SHAPE + 1
			if shapes == nil or shapes.desynced or index > SHAPE_SLOTS then
				error("unknown key set", 0)
			end
			local keys = shapes.keys[index]
			if not keys then
				error("unknown key set", 0)
			end
			local count = shapes.counts[index]
			local template = shapes.templates[index]
			if template == nil then
				-- Referred to while its own values were being read.
				template = {}
				for position = 1, count do
					template[keys[position]] = false
				end
				shapes.templates[index] = template
			end
			local result = table.clone(template)
			readValues(result, keys, count)
			return result
		elseif header == MAP_EMPTY then
			return {}
		end

		local keys: { string }
		local count: number
		local keyBytes: number
		if header <= MAP_JOINED_ADD then
			local joinedLength = readCount()
			local b, at = rbuff, rcursor
			if joinedLength > buffer.len(b) - at then
				error("malformed map", 0)
			end
			local joined = buffer.readstring(b, at, joinedLength)
			rcursor = at + joinedLength
			if header == MAP_JOINED and isU8 then
				return readJoinedU8(b, at, joined)
			end
			keys = if header == MAP_JOINED_ADD then {} else scratchReadKeys
			count = splitJoined(b, at, joined, keys)
			keyBytes = joinedLength - count + 1
		else
			count = readCount()
			if count > buffer.len(rbuff) - rcursor then
				error("count exceeds remaining data", 0)
			end
			keys = table.create(count)
			keyBytes = readStrings(keys, count)
		end

		local index: number? = nil
		if header == MAP_JOINED_ADD or header == MAP_LENGTHS_ADD then
			local shapes = rshapes
			local shapeSize = keyBytes + SHAPE_KEY_COST * count
			if shapes == nil or shapeSize > shapes.memory then
				error("unexpected key set", 0)
			end
			index = addShape(shapes, keys, count, shapeSize)
		end

		local result: { [string]: any } = if count <= rSizedCount then table.clone(rSized) else {}
		readValues(result, keys, count)
		resize(result, count)
		if index then
			local shapes = rshapes :: Shapes
			-- Unless its values' own key sets pushed it out already.
			if shapes.keys[index] == keys and shapes.templates[index] == nil then
				local template = table.clone(result)
				-- Values like tables, strings or Instances would stay alive here,
				-- outside the key-set memory's accounting: keep the keys only.
				if not plainValues then
					for position = 1, count do
						template[keys[position]] = false
					end
				end
				shapes.templates[index] = template
			end
		end
		return result
	end

	return {
		kind = "var",
		desc = `map<str,{value.desc}>`,
		minSize = 1,
		write = writeMap,
		read = readMap,
	} :: Codec
end

function Serdes.map(key: Codec, value: Codec): Codec
	assertCodec(key, "map's key type")
	assertCodec(value, "map's value type")
	if key == Serdes.string then
		return stringKeyMap(value)
	end
	local keyWrite, keyRead = key.write, key.read
	local valueWrite, valueRead = value.write, value.read
	local entryMinSize = key.minSize + value.minSize

	return {
		kind = "var",
		desc = `map<{key.desc},{value.desc}>`,
		minSize = 1,
		write = function(map: { [any]: any })
			local count = 0
			for _ in map do
				count += 1
			end
			writeVarUInt(count)
			for k, v in map do
				keyWrite(k)
				valueWrite(v)
			end
		end,
		read = function(): { [any]: any }
			local count = readVarUInt()
			checkCount(count, entryMinSize)
			local result = {}
			for _ = 1, count do
				local k = keyRead()
				local v = valueRead()
				-- An Instance key may not exist on this side; NaN can't be a key.
				if k ~= nil and k == k then
					result[k] = v
				end
			end
			return result
		end,
	}
end

-- BEGIN GENERATED by tools/structs.luau from tools/struct.template.luau: edit those, not this.
--[[
	Struct layout, with fields sorted by name so both sides agree without
	replicating anything:
		[flag bytes]   one bit per bool field and per optional field's presence
		[fixed fields] reserved in one go, written without bounds checks
		[strings]
		[other fields]
		[present optional fields' values]

	A struct is compiled into straight-line code when it's defined. Its first
	eight number fields become "slots": each has its key, offset and type
	fixed in upvalues, so reading or writing one is a lookup, one predictable
	type test and a buffer access, with no loop or table of field info to
	walk. Everything else (flags, vectors, CFrames, more than eight numbers,
	...) sits behind a single check, so a struct that doesn't use it pays
	one comparison for it. The slot blocks are unrolled by hand on purpose;
	each appears in both the single-value and the array version, because a
	Lua call per item would cost more than the item.
]]
function Serdes.struct(fields: { [string]: Codec }): Codec
	if type(fields) ~= "table" then
		error("[DogNet] struct expects a table of field types", 2)
	end

	local keys: { string } = {}
	for key in fields do
		if type(key) ~= "string" then
			error("[DogNet] struct field names must be strings", 2)
		end
		table.insert(keys, key)
	end
	table.sort(keys)

	local template: { [string]: any } = {}
	local descParts: { string } = {}

	local flagCount = 0
	local boolKeys: { string } = {}
	local boolBytes: { number } = {}
	local boolMasks: { number } = {}
	local optKeys: { string } = {}
	local optBytes: { number } = {}
	local optMasks: { number } = {}
	local optWriters: { (any) -> () } = {}
	local optReaders: { () -> any } = {}

	local numberKeys: { string } = {}
	local numberOffsets: { number } = {}
	local numberPrims: { number } = {}
	local vec3Keys: { string } = {}
	local vec3Offsets: { number } = {}
	local cframeKeys: { string } = {}
	local cframeOffsets: { number } = {}
	local otherKeys: { string } = {}
	local otherOffsets: { number } = {}
	local otherWriters: { (any) -> () } = {}
	local otherReaders: { () -> any } = {}
	local fixedSize = 0

	local stringKeys: { string } = {}
	local varKeys: { string } = {}
	local varWriters: { (any) -> () } = {}
	local varReaders: { () -> any } = {}
	local varMinSize = 0

	for _, key in keys do
		local codec = fields[key]
		assertCodec(codec, `struct field "{key}"`)
		template[key] = false
		table.insert(descParts, `{key}:{codec.desc}`)

		if codec.kind == "bool" or codec.kind == "optional" then
			local byte = flagCount // 8
			local mask = 2 ^ (flagCount % 8)
			flagCount += 1
			if codec.kind == "bool" then
				table.insert(boolKeys, key)
				table.insert(boolBytes, byte)
				table.insert(boolMasks, mask)
			else
				local inner = codec.inner :: Codec
				table.insert(optKeys, key)
				table.insert(optBytes, byte)
				table.insert(optMasks, mask)
				table.insert(optWriters, inner.write)
				table.insert(optReaders, inner.read)
			end
		elseif codec.size and codec.writeRaw then
			local prim = codec.prim
			if prim == PRIM_VEC3 then
				table.insert(vec3Keys, key)
				table.insert(vec3Offsets, fixedSize)
			elseif prim == PRIM_CFRAME then
				table.insert(cframeKeys, key)
				table.insert(cframeOffsets, fixedSize)
			elseif prim then
				table.insert(numberKeys, key)
				table.insert(numberOffsets, fixedSize)
				table.insert(numberPrims, prim)
			else
				table.insert(otherKeys, key)
				table.insert(otherOffsets, fixedSize)
				table.insert(otherWriters, codec.writeRaw)
				table.insert(otherReaders, codec.read)
			end
			fixedSize += codec.size
		elseif codec == Serdes.string then
			table.insert(stringKeys, key)
			varMinSize += 1
		else
			table.insert(varKeys, key)
			table.insert(varWriters, codec.write)
			table.insert(varReaders, codec.read)
			varMinSize += codec.minSize
		end
	end

	local flagBytes = (flagCount + 7) // 8
	local reserve = flagBytes + fixedSize
	local boolCount = #boolKeys
	local optCount = #optKeys
	local vec3Count = #vec3Keys
	local cframeCount = #cframeKeys
	local otherCount = #otherKeys
	local stringCount = #stringKeys
	local varCount = #varKeys
	local isFixed = stringCount == 0 and varCount == 0 and optCount == 0

	-- The slots: number fields 1 to 8. Unused slots are never reached.
	local numberCount = #numberKeys
	local k1: string, k2: string, k3: string, k4: string =
		numberKeys[1] or "", numberKeys[2] or "", numberKeys[3] or "", numberKeys[4] or ""
	local k5: string, k6: string, k7: string, k8: string =
		numberKeys[5] or "", numberKeys[6] or "", numberKeys[7] or "", numberKeys[8] or ""
	local o1: number, o2: number, o3: number, o4: number =
		numberOffsets[1] or 0, numberOffsets[2] or 0, numberOffsets[3] or 0, numberOffsets[4] or 0
	local o5: number, o6: number, o7: number, o8: number =
		numberOffsets[5] or 0, numberOffsets[6] or 0, numberOffsets[7] or 0, numberOffsets[8] or 0
	local p1: number, p2: number, p3: number, p4: number =
		numberPrims[1] or 0, numberPrims[2] or 0, numberPrims[3] or 0, numberPrims[4] or 0
	local p5: number, p6: number, p7: number, p8: number =
		numberPrims[5] or 0, numberPrims[6] or 0, numberPrims[7] or 0, numberPrims[8] or 0
	-- Number fields past the eighth.
	local extraKeys: { string } = table.move(numberKeys, 9, math.max(numberCount, 8), 1, {})
	local extraOffsets: { number } = table.move(numberOffsets, 9, math.max(numberCount, 8), 1, {})
	local extraPrims: { number } = table.move(numberPrims, 9, math.max(numberCount, 8), 1, {})
	local extraCount = #extraKeys
	-- Anything besides flags and the slots.
	local hasRest = extraCount > 0 or vec3Count > 0 or cframeCount > 0 or otherCount > 0

	-- Flags, vectors, CFrames, extra numbers and other fixed fields of `value`
	-- at `at` (flags) / `base` (fields). Only called when the struct has them.
	local function writeRest(value: { [string]: any }, b: buffer, at: number, base: number)
		if flagBytes == 1 then
			local byte = 0
			for index = 1, boolCount do
				if value[boolKeys[index]] then
					byte += boolMasks[index]
				end
			end
			for index = 1, optCount do
				if value[optKeys[index]] ~= nil then
					byte += optMasks[index]
				end
			end
			buffer.writeu8(b, at, byte)
		elseif flagBytes > 1 then
			buffer.fill(b, at, 0, flagBytes)
			for index = 1, boolCount do
				if value[boolKeys[index]] then
					local offset = at + boolBytes[index]
					buffer.writeu8(b, offset, buffer.readu8(b, offset) + boolMasks[index])
				end
			end
			for index = 1, optCount do
				if value[optKeys[index]] ~= nil then
					local offset = at + optBytes[index]
					buffer.writeu8(b, offset, buffer.readu8(b, offset) + optMasks[index])
				end
			end
		end
		for index = 1, extraCount do
			local field = value[extraKeys[index]]
			local offset = base + extraOffsets[index]
			local prim = extraPrims[index]
			if prim == PRIM_U8 then
				buffer.writeu8(b, offset, field)
			elseif prim == PRIM_F32 then
				buffer.writef32(b, offset, field)
			elseif prim == PRIM_U16 then
				buffer.writeu16(b, offset, field)
			elseif prim == PRIM_U32 then
				buffer.writeu32(b, offset, field)
			elseif prim == PRIM_I16 then
				buffer.writei16(b, offset, field)
			elseif prim == PRIM_I32 then
				buffer.writei32(b, offset, field)
			elseif prim == PRIM_I8 then
				buffer.writei8(b, offset, field)
			else
				buffer.writef64(b, offset, field)
			end
		end
		for index = 1, vec3Count do
			local vector = value[vec3Keys[index]]
			local offset = base + vec3Offsets[index]
			buffer.writef32(b, offset, vector.X)
			buffer.writef32(b, offset + 4, vector.Y)
			buffer.writef32(b, offset + 8, vector.Z)
		end
		for index = 1, cframeCount do
			writeCFrameAt(b, base + cframeOffsets[index], value[cframeKeys[index]])
		end
		for index = 1, otherCount do
			wcursor = base + otherOffsets[index]
			otherWriters[index](value[otherKeys[index]])
		end
	end

	-- Number fields past the eighth, and fixed fields of other types.
	local hasOdd = extraCount > 0 or otherCount > 0
	local function readOdd(result: { [string]: any }, b: buffer, base: number)
		for index = 1, extraCount do
			local offset = base + extraOffsets[index]
			local prim = extraPrims[index]
			if prim == PRIM_U8 then
				result[extraKeys[index]] = buffer.readu8(b, offset)
			elseif prim == PRIM_F32 then
				result[extraKeys[index]] = buffer.readf32(b, offset)
			elseif prim == PRIM_U16 then
				result[extraKeys[index]] = buffer.readu16(b, offset)
			elseif prim == PRIM_U32 then
				result[extraKeys[index]] = buffer.readu32(b, offset)
			elseif prim == PRIM_I16 then
				result[extraKeys[index]] = buffer.readi16(b, offset)
			elseif prim == PRIM_I32 then
				result[extraKeys[index]] = buffer.readi32(b, offset)
			elseif prim == PRIM_I8 then
				result[extraKeys[index]] = buffer.readi8(b, offset)
			else
				result[extraKeys[index]] = buffer.readf64(b, offset)
			end
		end
		for index = 1, otherCount do
			rcursor = base + otherOffsets[index]
			result[otherKeys[index]] = otherReaders[index]()
		end
	end

	-- Flags, vectors and CFrames, then the rest. `read` does the first three
	-- inline instead, since calls are what reading a single struct costs most.
	local function readRest(result: { [string]: any }, b: buffer, at: number, base: number)
		if flagBytes == 1 then
			local byte = buffer.readu8(b, at)
			for index = 1, boolCount do
				result[boolKeys[index]] = bit32.btest(byte, boolMasks[index])
			end
		elseif flagBytes > 1 then
			for index = 1, boolCount do
				result[boolKeys[index]] = bit32.btest(buffer.readu8(b, at + boolBytes[index]), boolMasks[index])
			end
		end
		for index = 1, vec3Count do
			local offset = base + vec3Offsets[index]
			result[vec3Keys[index]] =
				Vector3.new(buffer.readf32(b, offset), buffer.readf32(b, offset + 4), buffer.readf32(b, offset + 8))
		end
		for index = 1, cframeCount do
			result[cframeKeys[index]] = readCFrameAt(b, base + cframeOffsets[index])
		end
		if hasOdd then
			readOdd(result, b, base)
		end
	end

	local restOrFlags = hasRest or flagBytes > 0

	-- Flags and fixed fields at the write cursor; `reserve` bytes must be available.
	local function writeHead(value: { [string]: any })
		local b, at = wbuff, wcursor
		local base = at + flagBytes
		repeat
			if numberCount < 1 then
				break
			end
			local v1 = rawget(value, k1)
			if v1 == nil then
				v1 = value[k1]
			end
			if p1 == PRIM_U8 then
				buffer.writeu8(b, base + o1, v1)
			elseif p1 == PRIM_F32 then
				buffer.writef32(b, base + o1, v1)
			elseif p1 == PRIM_U16 then
				buffer.writeu16(b, base + o1, v1)
			elseif p1 == PRIM_U32 then
				buffer.writeu32(b, base + o1, v1)
			elseif p1 == PRIM_I16 then
				buffer.writei16(b, base + o1, v1)
			elseif p1 == PRIM_I32 then
				buffer.writei32(b, base + o1, v1)
			elseif p1 == PRIM_I8 then
				buffer.writei8(b, base + o1, v1)
			else
				buffer.writef64(b, base + o1, v1)
			end
			if numberCount < 2 then
				break
			end
			local v2 = rawget(value, k2)
			if v2 == nil then
				v2 = value[k2]
			end
			if p2 == PRIM_U8 then
				buffer.writeu8(b, base + o2, v2)
			elseif p2 == PRIM_F32 then
				buffer.writef32(b, base + o2, v2)
			elseif p2 == PRIM_U16 then
				buffer.writeu16(b, base + o2, v2)
			elseif p2 == PRIM_U32 then
				buffer.writeu32(b, base + o2, v2)
			elseif p2 == PRIM_I16 then
				buffer.writei16(b, base + o2, v2)
			elseif p2 == PRIM_I32 then
				buffer.writei32(b, base + o2, v2)
			elseif p2 == PRIM_I8 then
				buffer.writei8(b, base + o2, v2)
			else
				buffer.writef64(b, base + o2, v2)
			end
			if numberCount < 3 then
				break
			end
			local v3 = rawget(value, k3)
			if v3 == nil then
				v3 = value[k3]
			end
			if p3 == PRIM_U8 then
				buffer.writeu8(b, base + o3, v3)
			elseif p3 == PRIM_F32 then
				buffer.writef32(b, base + o3, v3)
			elseif p3 == PRIM_U16 then
				buffer.writeu16(b, base + o3, v3)
			elseif p3 == PRIM_U32 then
				buffer.writeu32(b, base + o3, v3)
			elseif p3 == PRIM_I16 then
				buffer.writei16(b, base + o3, v3)
			elseif p3 == PRIM_I32 then
				buffer.writei32(b, base + o3, v3)
			elseif p3 == PRIM_I8 then
				buffer.writei8(b, base + o3, v3)
			else
				buffer.writef64(b, base + o3, v3)
			end
			if numberCount < 4 then
				break
			end
			local v4 = rawget(value, k4)
			if v4 == nil then
				v4 = value[k4]
			end
			if p4 == PRIM_U8 then
				buffer.writeu8(b, base + o4, v4)
			elseif p4 == PRIM_F32 then
				buffer.writef32(b, base + o4, v4)
			elseif p4 == PRIM_U16 then
				buffer.writeu16(b, base + o4, v4)
			elseif p4 == PRIM_U32 then
				buffer.writeu32(b, base + o4, v4)
			elseif p4 == PRIM_I16 then
				buffer.writei16(b, base + o4, v4)
			elseif p4 == PRIM_I32 then
				buffer.writei32(b, base + o4, v4)
			elseif p4 == PRIM_I8 then
				buffer.writei8(b, base + o4, v4)
			else
				buffer.writef64(b, base + o4, v4)
			end
			if numberCount < 5 then
				break
			end
			local v5 = rawget(value, k5)
			if v5 == nil then
				v5 = value[k5]
			end
			if p5 == PRIM_U8 then
				buffer.writeu8(b, base + o5, v5)
			elseif p5 == PRIM_F32 then
				buffer.writef32(b, base + o5, v5)
			elseif p5 == PRIM_U16 then
				buffer.writeu16(b, base + o5, v5)
			elseif p5 == PRIM_U32 then
				buffer.writeu32(b, base + o5, v5)
			elseif p5 == PRIM_I16 then
				buffer.writei16(b, base + o5, v5)
			elseif p5 == PRIM_I32 then
				buffer.writei32(b, base + o5, v5)
			elseif p5 == PRIM_I8 then
				buffer.writei8(b, base + o5, v5)
			else
				buffer.writef64(b, base + o5, v5)
			end
			if numberCount < 6 then
				break
			end
			local v6 = rawget(value, k6)
			if v6 == nil then
				v6 = value[k6]
			end
			if p6 == PRIM_U8 then
				buffer.writeu8(b, base + o6, v6)
			elseif p6 == PRIM_F32 then
				buffer.writef32(b, base + o6, v6)
			elseif p6 == PRIM_U16 then
				buffer.writeu16(b, base + o6, v6)
			elseif p6 == PRIM_U32 then
				buffer.writeu32(b, base + o6, v6)
			elseif p6 == PRIM_I16 then
				buffer.writei16(b, base + o6, v6)
			elseif p6 == PRIM_I32 then
				buffer.writei32(b, base + o6, v6)
			elseif p6 == PRIM_I8 then
				buffer.writei8(b, base + o6, v6)
			else
				buffer.writef64(b, base + o6, v6)
			end
			if numberCount < 7 then
				break
			end
			local v7 = rawget(value, k7)
			if v7 == nil then
				v7 = value[k7]
			end
			if p7 == PRIM_U8 then
				buffer.writeu8(b, base + o7, v7)
			elseif p7 == PRIM_F32 then
				buffer.writef32(b, base + o7, v7)
			elseif p7 == PRIM_U16 then
				buffer.writeu16(b, base + o7, v7)
			elseif p7 == PRIM_U32 then
				buffer.writeu32(b, base + o7, v7)
			elseif p7 == PRIM_I16 then
				buffer.writei16(b, base + o7, v7)
			elseif p7 == PRIM_I32 then
				buffer.writei32(b, base + o7, v7)
			elseif p7 == PRIM_I8 then
				buffer.writei8(b, base + o7, v7)
			else
				buffer.writef64(b, base + o7, v7)
			end
			if numberCount < 8 then
				break
			end
			local v8 = rawget(value, k8)
			if v8 == nil then
				v8 = value[k8]
			end
			if p8 == PRIM_U8 then
				buffer.writeu8(b, base + o8, v8)
			elseif p8 == PRIM_F32 then
				buffer.writef32(b, base + o8, v8)
			elseif p8 == PRIM_U16 then
				buffer.writeu16(b, base + o8, v8)
			elseif p8 == PRIM_U32 then
				buffer.writeu32(b, base + o8, v8)
			elseif p8 == PRIM_I16 then
				buffer.writei16(b, base + o8, v8)
			elseif p8 == PRIM_I32 then
				buffer.writei32(b, base + o8, v8)
			elseif p8 == PRIM_I8 then
				buffer.writei8(b, base + o8, v8)
			else
				buffer.writef64(b, base + o8, v8)
			end
		until true
		if restOrFlags then
			writeRest(value, b, at, base)
		end
		wcursor = at + reserve
	end

	-- Structs made only of up to eight numbers of one type (u8, u16 or f32; a
	-- common shape for array items) get a loop per field count when in an
	-- array, with no per-field tests at all.
	local uniform = 0
	if not restOrFlags and numberCount > 0 then
		uniform = numberPrims[1]
		for index = 2, numberCount do
			if numberPrims[index] ~= uniform then
				uniform = 0
				break
			end
		end
	end

	-- `count` structs from `list`, back to back; the caller reserved the space.
	local function writeMany(list: { any }, count: number)
		local b, at = wbuff, wcursor
		if uniform == PRIM_U8 then
			local a1: string, a2: string, a3: string, a4: string, a5: string, a6: string, a7: string, a8: string =
				k1, k2, k3, k4, k5, k6, k7, k8
			if numberCount == 1 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu8(b, at, rawget(value, a1) or value[a1])
					at += 1
				end
			elseif numberCount == 2 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu8(b, at, rawget(value, a1) or value[a1])
					buffer.writeu8(b, at + 1, rawget(value, a2) or value[a2])
					at += 2
				end
			elseif numberCount == 3 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu8(b, at, rawget(value, a1) or value[a1])
					buffer.writeu8(b, at + 1, rawget(value, a2) or value[a2])
					buffer.writeu8(b, at + 2, rawget(value, a3) or value[a3])
					at += 3
				end
			elseif numberCount == 4 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu8(b, at, rawget(value, a1) or value[a1])
					buffer.writeu8(b, at + 1, rawget(value, a2) or value[a2])
					buffer.writeu8(b, at + 2, rawget(value, a3) or value[a3])
					buffer.writeu8(b, at + 3, rawget(value, a4) or value[a4])
					at += 4
				end
			elseif numberCount == 5 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu8(b, at, rawget(value, a1) or value[a1])
					buffer.writeu8(b, at + 1, rawget(value, a2) or value[a2])
					buffer.writeu8(b, at + 2, rawget(value, a3) or value[a3])
					buffer.writeu8(b, at + 3, rawget(value, a4) or value[a4])
					buffer.writeu8(b, at + 4, rawget(value, a5) or value[a5])
					at += 5
				end
			elseif numberCount == 6 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu8(b, at, rawget(value, a1) or value[a1])
					buffer.writeu8(b, at + 1, rawget(value, a2) or value[a2])
					buffer.writeu8(b, at + 2, rawget(value, a3) or value[a3])
					buffer.writeu8(b, at + 3, rawget(value, a4) or value[a4])
					buffer.writeu8(b, at + 4, rawget(value, a5) or value[a5])
					buffer.writeu8(b, at + 5, rawget(value, a6) or value[a6])
					at += 6
				end
			elseif numberCount == 7 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu8(b, at, rawget(value, a1) or value[a1])
					buffer.writeu8(b, at + 1, rawget(value, a2) or value[a2])
					buffer.writeu8(b, at + 2, rawget(value, a3) or value[a3])
					buffer.writeu8(b, at + 3, rawget(value, a4) or value[a4])
					buffer.writeu8(b, at + 4, rawget(value, a5) or value[a5])
					buffer.writeu8(b, at + 5, rawget(value, a6) or value[a6])
					buffer.writeu8(b, at + 6, rawget(value, a7) or value[a7])
					at += 7
				end
			elseif numberCount == 8 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu8(b, at, rawget(value, a1) or value[a1])
					buffer.writeu8(b, at + 1, rawget(value, a2) or value[a2])
					buffer.writeu8(b, at + 2, rawget(value, a3) or value[a3])
					buffer.writeu8(b, at + 3, rawget(value, a4) or value[a4])
					buffer.writeu8(b, at + 4, rawget(value, a5) or value[a5])
					buffer.writeu8(b, at + 5, rawget(value, a6) or value[a6])
					buffer.writeu8(b, at + 6, rawget(value, a7) or value[a7])
					buffer.writeu8(b, at + 7, rawget(value, a8) or value[a8])
					at += 8
				end
			end
		elseif uniform == PRIM_U16 then
			local a1: string, a2: string, a3: string, a4: string, a5: string, a6: string, a7: string, a8: string =
				k1, k2, k3, k4, k5, k6, k7, k8
			if numberCount == 1 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu16(b, at, rawget(value, a1) or value[a1])
					at += 2
				end
			elseif numberCount == 2 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu16(b, at, rawget(value, a1) or value[a1])
					buffer.writeu16(b, at + 2, rawget(value, a2) or value[a2])
					at += 4
				end
			elseif numberCount == 3 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu16(b, at, rawget(value, a1) or value[a1])
					buffer.writeu16(b, at + 2, rawget(value, a2) or value[a2])
					buffer.writeu16(b, at + 4, rawget(value, a3) or value[a3])
					at += 6
				end
			elseif numberCount == 4 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu16(b, at, rawget(value, a1) or value[a1])
					buffer.writeu16(b, at + 2, rawget(value, a2) or value[a2])
					buffer.writeu16(b, at + 4, rawget(value, a3) or value[a3])
					buffer.writeu16(b, at + 6, rawget(value, a4) or value[a4])
					at += 8
				end
			elseif numberCount == 5 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu16(b, at, rawget(value, a1) or value[a1])
					buffer.writeu16(b, at + 2, rawget(value, a2) or value[a2])
					buffer.writeu16(b, at + 4, rawget(value, a3) or value[a3])
					buffer.writeu16(b, at + 6, rawget(value, a4) or value[a4])
					buffer.writeu16(b, at + 8, rawget(value, a5) or value[a5])
					at += 10
				end
			elseif numberCount == 6 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu16(b, at, rawget(value, a1) or value[a1])
					buffer.writeu16(b, at + 2, rawget(value, a2) or value[a2])
					buffer.writeu16(b, at + 4, rawget(value, a3) or value[a3])
					buffer.writeu16(b, at + 6, rawget(value, a4) or value[a4])
					buffer.writeu16(b, at + 8, rawget(value, a5) or value[a5])
					buffer.writeu16(b, at + 10, rawget(value, a6) or value[a6])
					at += 12
				end
			elseif numberCount == 7 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu16(b, at, rawget(value, a1) or value[a1])
					buffer.writeu16(b, at + 2, rawget(value, a2) or value[a2])
					buffer.writeu16(b, at + 4, rawget(value, a3) or value[a3])
					buffer.writeu16(b, at + 6, rawget(value, a4) or value[a4])
					buffer.writeu16(b, at + 8, rawget(value, a5) or value[a5])
					buffer.writeu16(b, at + 10, rawget(value, a6) or value[a6])
					buffer.writeu16(b, at + 12, rawget(value, a7) or value[a7])
					at += 14
				end
			elseif numberCount == 8 then
				for item = 1, count do
					local value = list[item]
					buffer.writeu16(b, at, rawget(value, a1) or value[a1])
					buffer.writeu16(b, at + 2, rawget(value, a2) or value[a2])
					buffer.writeu16(b, at + 4, rawget(value, a3) or value[a3])
					buffer.writeu16(b, at + 6, rawget(value, a4) or value[a4])
					buffer.writeu16(b, at + 8, rawget(value, a5) or value[a5])
					buffer.writeu16(b, at + 10, rawget(value, a6) or value[a6])
					buffer.writeu16(b, at + 12, rawget(value, a7) or value[a7])
					buffer.writeu16(b, at + 14, rawget(value, a8) or value[a8])
					at += 16
				end
			end
		elseif uniform == PRIM_F32 then
			local a1: string, a2: string, a3: string, a4: string, a5: string, a6: string, a7: string, a8: string =
				k1, k2, k3, k4, k5, k6, k7, k8
			if numberCount == 1 then
				for item = 1, count do
					local value = list[item]
					buffer.writef32(b, at, rawget(value, a1) or value[a1])
					at += 4
				end
			elseif numberCount == 2 then
				for item = 1, count do
					local value = list[item]
					buffer.writef32(b, at, rawget(value, a1) or value[a1])
					buffer.writef32(b, at + 4, rawget(value, a2) or value[a2])
					at += 8
				end
			elseif numberCount == 3 then
				for item = 1, count do
					local value = list[item]
					buffer.writef32(b, at, rawget(value, a1) or value[a1])
					buffer.writef32(b, at + 4, rawget(value, a2) or value[a2])
					buffer.writef32(b, at + 8, rawget(value, a3) or value[a3])
					at += 12
				end
			elseif numberCount == 4 then
				for item = 1, count do
					local value = list[item]
					buffer.writef32(b, at, rawget(value, a1) or value[a1])
					buffer.writef32(b, at + 4, rawget(value, a2) or value[a2])
					buffer.writef32(b, at + 8, rawget(value, a3) or value[a3])
					buffer.writef32(b, at + 12, rawget(value, a4) or value[a4])
					at += 16
				end
			elseif numberCount == 5 then
				for item = 1, count do
					local value = list[item]
					buffer.writef32(b, at, rawget(value, a1) or value[a1])
					buffer.writef32(b, at + 4, rawget(value, a2) or value[a2])
					buffer.writef32(b, at + 8, rawget(value, a3) or value[a3])
					buffer.writef32(b, at + 12, rawget(value, a4) or value[a4])
					buffer.writef32(b, at + 16, rawget(value, a5) or value[a5])
					at += 20
				end
			elseif numberCount == 6 then
				for item = 1, count do
					local value = list[item]
					buffer.writef32(b, at, rawget(value, a1) or value[a1])
					buffer.writef32(b, at + 4, rawget(value, a2) or value[a2])
					buffer.writef32(b, at + 8, rawget(value, a3) or value[a3])
					buffer.writef32(b, at + 12, rawget(value, a4) or value[a4])
					buffer.writef32(b, at + 16, rawget(value, a5) or value[a5])
					buffer.writef32(b, at + 20, rawget(value, a6) or value[a6])
					at += 24
				end
			elseif numberCount == 7 then
				for item = 1, count do
					local value = list[item]
					buffer.writef32(b, at, rawget(value, a1) or value[a1])
					buffer.writef32(b, at + 4, rawget(value, a2) or value[a2])
					buffer.writef32(b, at + 8, rawget(value, a3) or value[a3])
					buffer.writef32(b, at + 12, rawget(value, a4) or value[a4])
					buffer.writef32(b, at + 16, rawget(value, a5) or value[a5])
					buffer.writef32(b, at + 20, rawget(value, a6) or value[a6])
					buffer.writef32(b, at + 24, rawget(value, a7) or value[a7])
					at += 28
				end
			elseif numberCount == 8 then
				for item = 1, count do
					local value = list[item]
					buffer.writef32(b, at, rawget(value, a1) or value[a1])
					buffer.writef32(b, at + 4, rawget(value, a2) or value[a2])
					buffer.writef32(b, at + 8, rawget(value, a3) or value[a3])
					buffer.writef32(b, at + 12, rawget(value, a4) or value[a4])
					buffer.writef32(b, at + 16, rawget(value, a5) or value[a5])
					buffer.writef32(b, at + 20, rawget(value, a6) or value[a6])
					buffer.writef32(b, at + 24, rawget(value, a7) or value[a7])
					buffer.writef32(b, at + 28, rawget(value, a8) or value[a8])
					at += 32
				end
			end
		else
			for item = 1, count do
				local value = list[item]
				local base = at + flagBytes
				repeat
					if numberCount < 1 then
						break
					end
					local v1 = rawget(value, k1)
					if v1 == nil then
						v1 = value[k1]
					end
					if p1 == PRIM_U8 then
						buffer.writeu8(b, base + o1, v1)
					elseif p1 == PRIM_F32 then
						buffer.writef32(b, base + o1, v1)
					elseif p1 == PRIM_U16 then
						buffer.writeu16(b, base + o1, v1)
					elseif p1 == PRIM_U32 then
						buffer.writeu32(b, base + o1, v1)
					elseif p1 == PRIM_I16 then
						buffer.writei16(b, base + o1, v1)
					elseif p1 == PRIM_I32 then
						buffer.writei32(b, base + o1, v1)
					elseif p1 == PRIM_I8 then
						buffer.writei8(b, base + o1, v1)
					else
						buffer.writef64(b, base + o1, v1)
					end
					if numberCount < 2 then
						break
					end
					local v2 = rawget(value, k2)
					if v2 == nil then
						v2 = value[k2]
					end
					if p2 == PRIM_U8 then
						buffer.writeu8(b, base + o2, v2)
					elseif p2 == PRIM_F32 then
						buffer.writef32(b, base + o2, v2)
					elseif p2 == PRIM_U16 then
						buffer.writeu16(b, base + o2, v2)
					elseif p2 == PRIM_U32 then
						buffer.writeu32(b, base + o2, v2)
					elseif p2 == PRIM_I16 then
						buffer.writei16(b, base + o2, v2)
					elseif p2 == PRIM_I32 then
						buffer.writei32(b, base + o2, v2)
					elseif p2 == PRIM_I8 then
						buffer.writei8(b, base + o2, v2)
					else
						buffer.writef64(b, base + o2, v2)
					end
					if numberCount < 3 then
						break
					end
					local v3 = rawget(value, k3)
					if v3 == nil then
						v3 = value[k3]
					end
					if p3 == PRIM_U8 then
						buffer.writeu8(b, base + o3, v3)
					elseif p3 == PRIM_F32 then
						buffer.writef32(b, base + o3, v3)
					elseif p3 == PRIM_U16 then
						buffer.writeu16(b, base + o3, v3)
					elseif p3 == PRIM_U32 then
						buffer.writeu32(b, base + o3, v3)
					elseif p3 == PRIM_I16 then
						buffer.writei16(b, base + o3, v3)
					elseif p3 == PRIM_I32 then
						buffer.writei32(b, base + o3, v3)
					elseif p3 == PRIM_I8 then
						buffer.writei8(b, base + o3, v3)
					else
						buffer.writef64(b, base + o3, v3)
					end
					if numberCount < 4 then
						break
					end
					local v4 = rawget(value, k4)
					if v4 == nil then
						v4 = value[k4]
					end
					if p4 == PRIM_U8 then
						buffer.writeu8(b, base + o4, v4)
					elseif p4 == PRIM_F32 then
						buffer.writef32(b, base + o4, v4)
					elseif p4 == PRIM_U16 then
						buffer.writeu16(b, base + o4, v4)
					elseif p4 == PRIM_U32 then
						buffer.writeu32(b, base + o4, v4)
					elseif p4 == PRIM_I16 then
						buffer.writei16(b, base + o4, v4)
					elseif p4 == PRIM_I32 then
						buffer.writei32(b, base + o4, v4)
					elseif p4 == PRIM_I8 then
						buffer.writei8(b, base + o4, v4)
					else
						buffer.writef64(b, base + o4, v4)
					end
					if numberCount < 5 then
						break
					end
					local v5 = rawget(value, k5)
					if v5 == nil then
						v5 = value[k5]
					end
					if p5 == PRIM_U8 then
						buffer.writeu8(b, base + o5, v5)
					elseif p5 == PRIM_F32 then
						buffer.writef32(b, base + o5, v5)
					elseif p5 == PRIM_U16 then
						buffer.writeu16(b, base + o5, v5)
					elseif p5 == PRIM_U32 then
						buffer.writeu32(b, base + o5, v5)
					elseif p5 == PRIM_I16 then
						buffer.writei16(b, base + o5, v5)
					elseif p5 == PRIM_I32 then
						buffer.writei32(b, base + o5, v5)
					elseif p5 == PRIM_I8 then
						buffer.writei8(b, base + o5, v5)
					else
						buffer.writef64(b, base + o5, v5)
					end
					if numberCount < 6 then
						break
					end
					local v6 = rawget(value, k6)
					if v6 == nil then
						v6 = value[k6]
					end
					if p6 == PRIM_U8 then
						buffer.writeu8(b, base + o6, v6)
					elseif p6 == PRIM_F32 then
						buffer.writef32(b, base + o6, v6)
					elseif p6 == PRIM_U16 then
						buffer.writeu16(b, base + o6, v6)
					elseif p6 == PRIM_U32 then
						buffer.writeu32(b, base + o6, v6)
					elseif p6 == PRIM_I16 then
						buffer.writei16(b, base + o6, v6)
					elseif p6 == PRIM_I32 then
						buffer.writei32(b, base + o6, v6)
					elseif p6 == PRIM_I8 then
						buffer.writei8(b, base + o6, v6)
					else
						buffer.writef64(b, base + o6, v6)
					end
					if numberCount < 7 then
						break
					end
					local v7 = rawget(value, k7)
					if v7 == nil then
						v7 = value[k7]
					end
					if p7 == PRIM_U8 then
						buffer.writeu8(b, base + o7, v7)
					elseif p7 == PRIM_F32 then
						buffer.writef32(b, base + o7, v7)
					elseif p7 == PRIM_U16 then
						buffer.writeu16(b, base + o7, v7)
					elseif p7 == PRIM_U32 then
						buffer.writeu32(b, base + o7, v7)
					elseif p7 == PRIM_I16 then
						buffer.writei16(b, base + o7, v7)
					elseif p7 == PRIM_I32 then
						buffer.writei32(b, base + o7, v7)
					elseif p7 == PRIM_I8 then
						buffer.writei8(b, base + o7, v7)
					else
						buffer.writef64(b, base + o7, v7)
					end
					if numberCount < 8 then
						break
					end
					local v8 = rawget(value, k8)
					if v8 == nil then
						v8 = value[k8]
					end
					if p8 == PRIM_U8 then
						buffer.writeu8(b, base + o8, v8)
					elseif p8 == PRIM_F32 then
						buffer.writef32(b, base + o8, v8)
					elseif p8 == PRIM_U16 then
						buffer.writeu16(b, base + o8, v8)
					elseif p8 == PRIM_U32 then
						buffer.writeu32(b, base + o8, v8)
					elseif p8 == PRIM_I16 then
						buffer.writei16(b, base + o8, v8)
					elseif p8 == PRIM_I32 then
						buffer.writei32(b, base + o8, v8)
					elseif p8 == PRIM_I8 then
						buffer.writei8(b, base + o8, v8)
					else
						buffer.writef64(b, base + o8, v8)
					end
				until true
				if restOrFlags then
					writeRest(value, b, at, base)
				end
				at += reserve
			end
		end
		wcursor = at
	end

	local write
	if isFixed then
		write = function(value: { [string]: any })
			if wcursor + reserve > wsize then
				grow(reserve)
			end
			writeHead(value)
		end
	else
		write = function(value: { [string]: any })
			if wcursor + reserve > wsize then
				grow(reserve)
			end
			writeHead(value)
			for index = 1, stringCount do
				writeString(value[stringKeys[index]])
			end
			for index = 1, varCount do
				varWriters[index](value[varKeys[index]])
			end
			for index = 1, optCount do
				local field = value[optKeys[index]]
				if field ~= nil then
					optWriters[index](field)
				end
			end
		end
	end

	local function read(): { [string]: any }
		-- Cloning a template with the same keys pre-sizes the hash part.
		local result = table.clone(template)
		local b, at = rbuff, rcursor
		local base = at + flagBytes
		repeat
			if numberCount < 1 then
				break
			end
			if p1 == PRIM_U8 then
				result[k1] = buffer.readu8(b, base + o1)
			elseif p1 == PRIM_F32 then
				result[k1] = buffer.readf32(b, base + o1)
			elseif p1 == PRIM_U16 then
				result[k1] = buffer.readu16(b, base + o1)
			elseif p1 == PRIM_U32 then
				result[k1] = buffer.readu32(b, base + o1)
			elseif p1 == PRIM_I16 then
				result[k1] = buffer.readi16(b, base + o1)
			elseif p1 == PRIM_I32 then
				result[k1] = buffer.readi32(b, base + o1)
			elseif p1 == PRIM_I8 then
				result[k1] = buffer.readi8(b, base + o1)
			else
				result[k1] = buffer.readf64(b, base + o1)
			end
			if numberCount < 2 then
				break
			end
			if p2 == PRIM_U8 then
				result[k2] = buffer.readu8(b, base + o2)
			elseif p2 == PRIM_F32 then
				result[k2] = buffer.readf32(b, base + o2)
			elseif p2 == PRIM_U16 then
				result[k2] = buffer.readu16(b, base + o2)
			elseif p2 == PRIM_U32 then
				result[k2] = buffer.readu32(b, base + o2)
			elseif p2 == PRIM_I16 then
				result[k2] = buffer.readi16(b, base + o2)
			elseif p2 == PRIM_I32 then
				result[k2] = buffer.readi32(b, base + o2)
			elseif p2 == PRIM_I8 then
				result[k2] = buffer.readi8(b, base + o2)
			else
				result[k2] = buffer.readf64(b, base + o2)
			end
			if numberCount < 3 then
				break
			end
			if p3 == PRIM_U8 then
				result[k3] = buffer.readu8(b, base + o3)
			elseif p3 == PRIM_F32 then
				result[k3] = buffer.readf32(b, base + o3)
			elseif p3 == PRIM_U16 then
				result[k3] = buffer.readu16(b, base + o3)
			elseif p3 == PRIM_U32 then
				result[k3] = buffer.readu32(b, base + o3)
			elseif p3 == PRIM_I16 then
				result[k3] = buffer.readi16(b, base + o3)
			elseif p3 == PRIM_I32 then
				result[k3] = buffer.readi32(b, base + o3)
			elseif p3 == PRIM_I8 then
				result[k3] = buffer.readi8(b, base + o3)
			else
				result[k3] = buffer.readf64(b, base + o3)
			end
			if numberCount < 4 then
				break
			end
			if p4 == PRIM_U8 then
				result[k4] = buffer.readu8(b, base + o4)
			elseif p4 == PRIM_F32 then
				result[k4] = buffer.readf32(b, base + o4)
			elseif p4 == PRIM_U16 then
				result[k4] = buffer.readu16(b, base + o4)
			elseif p4 == PRIM_U32 then
				result[k4] = buffer.readu32(b, base + o4)
			elseif p4 == PRIM_I16 then
				result[k4] = buffer.readi16(b, base + o4)
			elseif p4 == PRIM_I32 then
				result[k4] = buffer.readi32(b, base + o4)
			elseif p4 == PRIM_I8 then
				result[k4] = buffer.readi8(b, base + o4)
			else
				result[k4] = buffer.readf64(b, base + o4)
			end
			if numberCount < 5 then
				break
			end
			if p5 == PRIM_U8 then
				result[k5] = buffer.readu8(b, base + o5)
			elseif p5 == PRIM_F32 then
				result[k5] = buffer.readf32(b, base + o5)
			elseif p5 == PRIM_U16 then
				result[k5] = buffer.readu16(b, base + o5)
			elseif p5 == PRIM_U32 then
				result[k5] = buffer.readu32(b, base + o5)
			elseif p5 == PRIM_I16 then
				result[k5] = buffer.readi16(b, base + o5)
			elseif p5 == PRIM_I32 then
				result[k5] = buffer.readi32(b, base + o5)
			elseif p5 == PRIM_I8 then
				result[k5] = buffer.readi8(b, base + o5)
			else
				result[k5] = buffer.readf64(b, base + o5)
			end
			if numberCount < 6 then
				break
			end
			if p6 == PRIM_U8 then
				result[k6] = buffer.readu8(b, base + o6)
			elseif p6 == PRIM_F32 then
				result[k6] = buffer.readf32(b, base + o6)
			elseif p6 == PRIM_U16 then
				result[k6] = buffer.readu16(b, base + o6)
			elseif p6 == PRIM_U32 then
				result[k6] = buffer.readu32(b, base + o6)
			elseif p6 == PRIM_I16 then
				result[k6] = buffer.readi16(b, base + o6)
			elseif p6 == PRIM_I32 then
				result[k6] = buffer.readi32(b, base + o6)
			elseif p6 == PRIM_I8 then
				result[k6] = buffer.readi8(b, base + o6)
			else
				result[k6] = buffer.readf64(b, base + o6)
			end
			if numberCount < 7 then
				break
			end
			if p7 == PRIM_U8 then
				result[k7] = buffer.readu8(b, base + o7)
			elseif p7 == PRIM_F32 then
				result[k7] = buffer.readf32(b, base + o7)
			elseif p7 == PRIM_U16 then
				result[k7] = buffer.readu16(b, base + o7)
			elseif p7 == PRIM_U32 then
				result[k7] = buffer.readu32(b, base + o7)
			elseif p7 == PRIM_I16 then
				result[k7] = buffer.readi16(b, base + o7)
			elseif p7 == PRIM_I32 then
				result[k7] = buffer.readi32(b, base + o7)
			elseif p7 == PRIM_I8 then
				result[k7] = buffer.readi8(b, base + o7)
			else
				result[k7] = buffer.readf64(b, base + o7)
			end
			if numberCount < 8 then
				break
			end
			if p8 == PRIM_U8 then
				result[k8] = buffer.readu8(b, base + o8)
			elseif p8 == PRIM_F32 then
				result[k8] = buffer.readf32(b, base + o8)
			elseif p8 == PRIM_U16 then
				result[k8] = buffer.readu16(b, base + o8)
			elseif p8 == PRIM_U32 then
				result[k8] = buffer.readu32(b, base + o8)
			elseif p8 == PRIM_I16 then
				result[k8] = buffer.readi16(b, base + o8)
			elseif p8 == PRIM_I32 then
				result[k8] = buffer.readi32(b, base + o8)
			elseif p8 == PRIM_I8 then
				result[k8] = buffer.readi8(b, base + o8)
			else
				result[k8] = buffer.readf64(b, base + o8)
			end
		until true
		if restOrFlags then
			if flagBytes == 1 then
				local byte = buffer.readu8(b, at)
				for index = 1, boolCount do
					result[boolKeys[index]] = bit32.btest(byte, boolMasks[index])
				end
			elseif flagBytes > 1 then
				for index = 1, boolCount do
					result[boolKeys[index]] = bit32.btest(buffer.readu8(b, at + boolBytes[index]), boolMasks[index])
				end
			end
			for index = 1, vec3Count do
				local offset = base + vec3Offsets[index]
				result[vec3Keys[index]] =
					Vector3.new(buffer.readf32(b, offset), buffer.readf32(b, offset + 4), buffer.readf32(b, offset + 8))
			end
			for index = 1, cframeCount do
				-- readCFrameAt, inline.
				local offset = base + cframeOffsets[index]
				local x, y, z = buffer.readf32(b, offset), buffer.readf32(b, offset + 4), buffer.readf32(b, offset + 8)
				local rx, ry, rz =
					buffer.readf32(b, offset + 12), buffer.readf32(b, offset + 16), buffer.readf32(b, offset + 20)
				local angle = math.sqrt(rx * rx + ry * ry + rz * rz)
				if angle > 0 then
					local half = angle * 0.5
					local scale = math.sin(half) / angle
					result[cframeKeys[index]] = CFrame.new(x, y, z, rx * scale, ry * scale, rz * scale, math.cos(half))
				else
					result[cframeKeys[index]] = CFrame.new(x, y, z)
				end
			end
			if hasOdd then
				readOdd(result, b, base)
			end
		end

		if isFixed then
			rcursor = at + reserve
			return result
		end
		local cursor = at + reserve
		for index = 1, stringCount do
			local length = buffer.readu8(b, cursor)
			if length < 128 then
				cursor += 1
			else
				rcursor = cursor
				length = readVarUInt()
				cursor = rcursor
			end
			result[stringKeys[index]] = buffer.readstring(b, cursor, length)
			cursor += length
		end
		rcursor = cursor
		for index = 1, varCount do
			result[varKeys[index]] = varReaders[index]()
		end
		for index = 1, optCount do
			if bit32.btest(buffer.readu8(b, at + optBytes[index]), optMasks[index]) then
				result[optKeys[index]] = optReaders[index]()
			else
				result[optKeys[index]] = nil
			end
		end
		return result
	end

	local function readMany(list: { any }, count: number)
		local b, at = rbuff, rcursor
		if count * reserve > buffer.len(b) - at then
			error("count exceeds remaining data", 0)
		end
		if uniform == PRIM_U8 then
			local a1: string, a2: string, a3: string, a4: string, a5: string, a6: string, a7: string, a8: string =
				k1, k2, k3, k4, k5, k6, k7, k8
			if numberCount == 1 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu8(b, at)
					list[item] = result
					at += 1
				end
			elseif numberCount == 2 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu8(b, at)
					result[a2] = buffer.readu8(b, at + 1)
					list[item] = result
					at += 2
				end
			elseif numberCount == 3 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu8(b, at)
					result[a2] = buffer.readu8(b, at + 1)
					result[a3] = buffer.readu8(b, at + 2)
					list[item] = result
					at += 3
				end
			elseif numberCount == 4 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu8(b, at)
					result[a2] = buffer.readu8(b, at + 1)
					result[a3] = buffer.readu8(b, at + 2)
					result[a4] = buffer.readu8(b, at + 3)
					list[item] = result
					at += 4
				end
			elseif numberCount == 5 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu8(b, at)
					result[a2] = buffer.readu8(b, at + 1)
					result[a3] = buffer.readu8(b, at + 2)
					result[a4] = buffer.readu8(b, at + 3)
					result[a5] = buffer.readu8(b, at + 4)
					list[item] = result
					at += 5
				end
			elseif numberCount == 6 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu8(b, at)
					result[a2] = buffer.readu8(b, at + 1)
					result[a3] = buffer.readu8(b, at + 2)
					result[a4] = buffer.readu8(b, at + 3)
					result[a5] = buffer.readu8(b, at + 4)
					result[a6] = buffer.readu8(b, at + 5)
					list[item] = result
					at += 6
				end
			elseif numberCount == 7 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu8(b, at)
					result[a2] = buffer.readu8(b, at + 1)
					result[a3] = buffer.readu8(b, at + 2)
					result[a4] = buffer.readu8(b, at + 3)
					result[a5] = buffer.readu8(b, at + 4)
					result[a6] = buffer.readu8(b, at + 5)
					result[a7] = buffer.readu8(b, at + 6)
					list[item] = result
					at += 7
				end
			elseif numberCount == 8 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu8(b, at)
					result[a2] = buffer.readu8(b, at + 1)
					result[a3] = buffer.readu8(b, at + 2)
					result[a4] = buffer.readu8(b, at + 3)
					result[a5] = buffer.readu8(b, at + 4)
					result[a6] = buffer.readu8(b, at + 5)
					result[a7] = buffer.readu8(b, at + 6)
					result[a8] = buffer.readu8(b, at + 7)
					list[item] = result
					at += 8
				end
			end
		elseif uniform == PRIM_U16 then
			local a1: string, a2: string, a3: string, a4: string, a5: string, a6: string, a7: string, a8: string =
				k1, k2, k3, k4, k5, k6, k7, k8
			if numberCount == 1 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu16(b, at)
					list[item] = result
					at += 2
				end
			elseif numberCount == 2 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu16(b, at)
					result[a2] = buffer.readu16(b, at + 2)
					list[item] = result
					at += 4
				end
			elseif numberCount == 3 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu16(b, at)
					result[a2] = buffer.readu16(b, at + 2)
					result[a3] = buffer.readu16(b, at + 4)
					list[item] = result
					at += 6
				end
			elseif numberCount == 4 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu16(b, at)
					result[a2] = buffer.readu16(b, at + 2)
					result[a3] = buffer.readu16(b, at + 4)
					result[a4] = buffer.readu16(b, at + 6)
					list[item] = result
					at += 8
				end
			elseif numberCount == 5 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu16(b, at)
					result[a2] = buffer.readu16(b, at + 2)
					result[a3] = buffer.readu16(b, at + 4)
					result[a4] = buffer.readu16(b, at + 6)
					result[a5] = buffer.readu16(b, at + 8)
					list[item] = result
					at += 10
				end
			elseif numberCount == 6 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu16(b, at)
					result[a2] = buffer.readu16(b, at + 2)
					result[a3] = buffer.readu16(b, at + 4)
					result[a4] = buffer.readu16(b, at + 6)
					result[a5] = buffer.readu16(b, at + 8)
					result[a6] = buffer.readu16(b, at + 10)
					list[item] = result
					at += 12
				end
			elseif numberCount == 7 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu16(b, at)
					result[a2] = buffer.readu16(b, at + 2)
					result[a3] = buffer.readu16(b, at + 4)
					result[a4] = buffer.readu16(b, at + 6)
					result[a5] = buffer.readu16(b, at + 8)
					result[a6] = buffer.readu16(b, at + 10)
					result[a7] = buffer.readu16(b, at + 12)
					list[item] = result
					at += 14
				end
			elseif numberCount == 8 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readu16(b, at)
					result[a2] = buffer.readu16(b, at + 2)
					result[a3] = buffer.readu16(b, at + 4)
					result[a4] = buffer.readu16(b, at + 6)
					result[a5] = buffer.readu16(b, at + 8)
					result[a6] = buffer.readu16(b, at + 10)
					result[a7] = buffer.readu16(b, at + 12)
					result[a8] = buffer.readu16(b, at + 14)
					list[item] = result
					at += 16
				end
			end
		elseif uniform == PRIM_F32 then
			local a1: string, a2: string, a3: string, a4: string, a5: string, a6: string, a7: string, a8: string =
				k1, k2, k3, k4, k5, k6, k7, k8
			if numberCount == 1 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readf32(b, at)
					list[item] = result
					at += 4
				end
			elseif numberCount == 2 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readf32(b, at)
					result[a2] = buffer.readf32(b, at + 4)
					list[item] = result
					at += 8
				end
			elseif numberCount == 3 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readf32(b, at)
					result[a2] = buffer.readf32(b, at + 4)
					result[a3] = buffer.readf32(b, at + 8)
					list[item] = result
					at += 12
				end
			elseif numberCount == 4 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readf32(b, at)
					result[a2] = buffer.readf32(b, at + 4)
					result[a3] = buffer.readf32(b, at + 8)
					result[a4] = buffer.readf32(b, at + 12)
					list[item] = result
					at += 16
				end
			elseif numberCount == 5 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readf32(b, at)
					result[a2] = buffer.readf32(b, at + 4)
					result[a3] = buffer.readf32(b, at + 8)
					result[a4] = buffer.readf32(b, at + 12)
					result[a5] = buffer.readf32(b, at + 16)
					list[item] = result
					at += 20
				end
			elseif numberCount == 6 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readf32(b, at)
					result[a2] = buffer.readf32(b, at + 4)
					result[a3] = buffer.readf32(b, at + 8)
					result[a4] = buffer.readf32(b, at + 12)
					result[a5] = buffer.readf32(b, at + 16)
					result[a6] = buffer.readf32(b, at + 20)
					list[item] = result
					at += 24
				end
			elseif numberCount == 7 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readf32(b, at)
					result[a2] = buffer.readf32(b, at + 4)
					result[a3] = buffer.readf32(b, at + 8)
					result[a4] = buffer.readf32(b, at + 12)
					result[a5] = buffer.readf32(b, at + 16)
					result[a6] = buffer.readf32(b, at + 20)
					result[a7] = buffer.readf32(b, at + 24)
					list[item] = result
					at += 28
				end
			elseif numberCount == 8 then
				for item = 1, count do
					local result = table.clone(template)
					result[a1] = buffer.readf32(b, at)
					result[a2] = buffer.readf32(b, at + 4)
					result[a3] = buffer.readf32(b, at + 8)
					result[a4] = buffer.readf32(b, at + 12)
					result[a5] = buffer.readf32(b, at + 16)
					result[a6] = buffer.readf32(b, at + 20)
					result[a7] = buffer.readf32(b, at + 24)
					result[a8] = buffer.readf32(b, at + 28)
					list[item] = result
					at += 32
				end
			end
		else
			for item = 1, count do
				local result = table.clone(template)
				local base = at + flagBytes
				repeat
					if numberCount < 1 then
						break
					end
					if p1 == PRIM_U8 then
						result[k1] = buffer.readu8(b, base + o1)
					elseif p1 == PRIM_F32 then
						result[k1] = buffer.readf32(b, base + o1)
					elseif p1 == PRIM_U16 then
						result[k1] = buffer.readu16(b, base + o1)
					elseif p1 == PRIM_U32 then
						result[k1] = buffer.readu32(b, base + o1)
					elseif p1 == PRIM_I16 then
						result[k1] = buffer.readi16(b, base + o1)
					elseif p1 == PRIM_I32 then
						result[k1] = buffer.readi32(b, base + o1)
					elseif p1 == PRIM_I8 then
						result[k1] = buffer.readi8(b, base + o1)
					else
						result[k1] = buffer.readf64(b, base + o1)
					end
					if numberCount < 2 then
						break
					end
					if p2 == PRIM_U8 then
						result[k2] = buffer.readu8(b, base + o2)
					elseif p2 == PRIM_F32 then
						result[k2] = buffer.readf32(b, base + o2)
					elseif p2 == PRIM_U16 then
						result[k2] = buffer.readu16(b, base + o2)
					elseif p2 == PRIM_U32 then
						result[k2] = buffer.readu32(b, base + o2)
					elseif p2 == PRIM_I16 then
						result[k2] = buffer.readi16(b, base + o2)
					elseif p2 == PRIM_I32 then
						result[k2] = buffer.readi32(b, base + o2)
					elseif p2 == PRIM_I8 then
						result[k2] = buffer.readi8(b, base + o2)
					else
						result[k2] = buffer.readf64(b, base + o2)
					end
					if numberCount < 3 then
						break
					end
					if p3 == PRIM_U8 then
						result[k3] = buffer.readu8(b, base + o3)
					elseif p3 == PRIM_F32 then
						result[k3] = buffer.readf32(b, base + o3)
					elseif p3 == PRIM_U16 then
						result[k3] = buffer.readu16(b, base + o3)
					elseif p3 == PRIM_U32 then
						result[k3] = buffer.readu32(b, base + o3)
					elseif p3 == PRIM_I16 then
						result[k3] = buffer.readi16(b, base + o3)
					elseif p3 == PRIM_I32 then
						result[k3] = buffer.readi32(b, base + o3)
					elseif p3 == PRIM_I8 then
						result[k3] = buffer.readi8(b, base + o3)
					else
						result[k3] = buffer.readf64(b, base + o3)
					end
					if numberCount < 4 then
						break
					end
					if p4 == PRIM_U8 then
						result[k4] = buffer.readu8(b, base + o4)
					elseif p4 == PRIM_F32 then
						result[k4] = buffer.readf32(b, base + o4)
					elseif p4 == PRIM_U16 then
						result[k4] = buffer.readu16(b, base + o4)
					elseif p4 == PRIM_U32 then
						result[k4] = buffer.readu32(b, base + o4)
					elseif p4 == PRIM_I16 then
						result[k4] = buffer.readi16(b, base + o4)
					elseif p4 == PRIM_I32 then
						result[k4] = buffer.readi32(b, base + o4)
					elseif p4 == PRIM_I8 then
						result[k4] = buffer.readi8(b, base + o4)
					else
						result[k4] = buffer.readf64(b, base + o4)
					end
					if numberCount < 5 then
						break
					end
					if p5 == PRIM_U8 then
						result[k5] = buffer.readu8(b, base + o5)
					elseif p5 == PRIM_F32 then
						result[k5] = buffer.readf32(b, base + o5)
					elseif p5 == PRIM_U16 then
						result[k5] = buffer.readu16(b, base + o5)
					elseif p5 == PRIM_U32 then
						result[k5] = buffer.readu32(b, base + o5)
					elseif p5 == PRIM_I16 then
						result[k5] = buffer.readi16(b, base + o5)
					elseif p5 == PRIM_I32 then
						result[k5] = buffer.readi32(b, base + o5)
					elseif p5 == PRIM_I8 then
						result[k5] = buffer.readi8(b, base + o5)
					else
						result[k5] = buffer.readf64(b, base + o5)
					end
					if numberCount < 6 then
						break
					end
					if p6 == PRIM_U8 then
						result[k6] = buffer.readu8(b, base + o6)
					elseif p6 == PRIM_F32 then
						result[k6] = buffer.readf32(b, base + o6)
					elseif p6 == PRIM_U16 then
						result[k6] = buffer.readu16(b, base + o6)
					elseif p6 == PRIM_U32 then
						result[k6] = buffer.readu32(b, base + o6)
					elseif p6 == PRIM_I16 then
						result[k6] = buffer.readi16(b, base + o6)
					elseif p6 == PRIM_I32 then
						result[k6] = buffer.readi32(b, base + o6)
					elseif p6 == PRIM_I8 then
						result[k6] = buffer.readi8(b, base + o6)
					else
						result[k6] = buffer.readf64(b, base + o6)
					end
					if numberCount < 7 then
						break
					end
					if p7 == PRIM_U8 then
						result[k7] = buffer.readu8(b, base + o7)
					elseif p7 == PRIM_F32 then
						result[k7] = buffer.readf32(b, base + o7)
					elseif p7 == PRIM_U16 then
						result[k7] = buffer.readu16(b, base + o7)
					elseif p7 == PRIM_U32 then
						result[k7] = buffer.readu32(b, base + o7)
					elseif p7 == PRIM_I16 then
						result[k7] = buffer.readi16(b, base + o7)
					elseif p7 == PRIM_I32 then
						result[k7] = buffer.readi32(b, base + o7)
					elseif p7 == PRIM_I8 then
						result[k7] = buffer.readi8(b, base + o7)
					else
						result[k7] = buffer.readf64(b, base + o7)
					end
					if numberCount < 8 then
						break
					end
					if p8 == PRIM_U8 then
						result[k8] = buffer.readu8(b, base + o8)
					elseif p8 == PRIM_F32 then
						result[k8] = buffer.readf32(b, base + o8)
					elseif p8 == PRIM_U16 then
						result[k8] = buffer.readu16(b, base + o8)
					elseif p8 == PRIM_U32 then
						result[k8] = buffer.readu32(b, base + o8)
					elseif p8 == PRIM_I16 then
						result[k8] = buffer.readi16(b, base + o8)
					elseif p8 == PRIM_I32 then
						result[k8] = buffer.readi32(b, base + o8)
					elseif p8 == PRIM_I8 then
						result[k8] = buffer.readi8(b, base + o8)
					else
						result[k8] = buffer.readf64(b, base + o8)
					end
				until true
				if restOrFlags then
					readRest(result, b, at, base)
				end
				list[item] = result
				at += reserve
			end
		end
		rcursor = at
	end

	return {
		kind = "struct",
		desc = "{" .. table.concat(descParts, ",") .. "}",
		size = if isFixed then reserve else nil,
		writeRaw = if isFixed then writeHead else nil,
		minSize = reserve + varMinSize,
		write = write,
		read = read,
		writeMany = if isFixed then writeMany else nil,
		readMany = if isFixed then readMany else nil,
	}
end

-- END GENERATED by tools/structs.luau
--------------------------------------------------------------------------------
-- auto: picks an encoding at runtime from the value's type
--------------------------------------------------------------------------------

local TAG_NIL = 0
local TAG_FALSE = 1
local TAG_TRUE = 2
local TAG_U8 = 3
local TAG_U16 = 4
local TAG_U32 = 5
local TAG_NEG_U8 = 6
local TAG_NEG_U16 = 7
local TAG_NEG_U32 = 8
local TAG_F32 = 9
local TAG_F64 = 10
local TAG_STRING = 11
local TAG_VECTOR2 = 12
local TAG_VECTOR3 = 13
local TAG_COLOR3 = 14
local TAG_CFRAME = 15
local TAG_BUFFER = 16
local TAG_INSTANCE = 17
local TAG_ARRAY = 18
local TAG_DICTIONARY = 19
local TAG_REF = 20
-- Tags 128..255 are the integers 0..127, stored in the tag byte itself.
local TAG_SMALL_INT = 128

local f32Scratch = buffer.create(4)

local function writeTag(tag: number)
	if wcursor + 1 > wsize then
		grow(1)
	end
	buffer.writeu8(wbuff, wcursor, tag)
	wcursor += 1
end

local function autoWrite(value: any, depth: number)
	local kind = typeof(value)

	if kind == "number" then
		if value % 1 == 0 and value >= -4294967295 and value <= 4294967295 then
			if wcursor + 5 > wsize then
				grow(5)
			end
			local magnitude = math.abs(value)
			local negative = value < 0
			if not negative and value < 128 then
				buffer.writeu8(wbuff, wcursor, TAG_SMALL_INT + value)
				wcursor += 1
			elseif magnitude < 256 then
				buffer.writeu8(wbuff, wcursor, if negative then TAG_NEG_U8 else TAG_U8)
				buffer.writeu8(wbuff, wcursor + 1, magnitude)
				wcursor += 2
			elseif magnitude < 65536 then
				buffer.writeu8(wbuff, wcursor, if negative then TAG_NEG_U16 else TAG_U16)
				buffer.writeu16(wbuff, wcursor + 1, magnitude)
				wcursor += 3
			else
				buffer.writeu8(wbuff, wcursor, if negative then TAG_NEG_U32 else TAG_U32)
				buffer.writeu32(wbuff, wcursor + 1, magnitude)
				wcursor += 5
			end
			return
		end

		if wcursor + 9 > wsize then
			grow(9)
		end
		buffer.writef32(f32Scratch, 0, value)
		if buffer.readf32(f32Scratch, 0) == value then
			buffer.writeu8(wbuff, wcursor, TAG_F32)
			buffer.writef32(wbuff, wcursor + 1, value)
			wcursor += 5
		else
			buffer.writeu8(wbuff, wcursor, TAG_F64)
			buffer.writef64(wbuff, wcursor + 1, value)
			wcursor += 9
		end
	elseif kind == "string" then
		writeTag(TAG_STRING)
		writeString(value)
	elseif kind == "boolean" then
		writeTag(if value then TAG_TRUE else TAG_FALSE)
	elseif kind == "nil" then
		writeTag(TAG_NIL)
	elseif kind == "table" then
		if depth >= AUTO_MAX_DEPTH then
			error("[DogNet] auto: tables nested too deeply (is one of them cyclic?)", 0)
		end
		local length = #value
		local count = 0
		for _ in value do
			count += 1
		end
		if count == length then
			writeTag(TAG_ARRAY)
			writeVarUInt(length)
			for index = 1, length do
				autoWrite(value[index], depth + 1)
			end
		else
			writeTag(TAG_DICTIONARY)
			writeVarUInt(count)
			for k, v in value do
				autoWrite(k, depth + 1)
				autoWrite(v, depth + 1)
			end
		end
	elseif kind == "Vector3" then
		writeTag(TAG_VECTOR3)
		Serdes.vec3.write(value)
	elseif kind == "CFrame" then
		writeTag(TAG_CFRAME)
		Serdes.cframe.write(value)
	elseif kind == "Vector2" then
		writeTag(TAG_VECTOR2)
		Serdes.vec2.write(value)
	elseif kind == "Color3" then
		writeTag(TAG_COLOR3)
		Serdes.color3.write(value)
	elseif kind == "buffer" then
		writeTag(TAG_BUFFER)
		Serdes.buff.write(value)
	elseif kind == "Instance" then
		writeTag(TAG_INSTANCE)
		wrefCount += 1
		wrefs[wrefCount] = value
	else
		-- EnumItem, UDim2, NumberRange...: let Roblox serialize it.
		writeTag(TAG_REF)
		wrefCount += 1
		wrefs[wrefCount] = value
	end
end

local autoReaders: { [number]: (depth: number) -> any } = {}
local function autoRead(depth: number): any
	local tag = buffer.readu8(rbuff, rcursor)
	rcursor += 1
	if tag >= TAG_SMALL_INT then
		return tag - TAG_SMALL_INT
	end
	local reader = autoReaders[tag]
	if reader == nil then
		error(`auto: unknown tag {tag}`, 0)
	end
	return reader(depth)
end

autoReaders[TAG_NIL] = function()
	return nil
end
autoReaders[TAG_FALSE] = function()
	return false
end
autoReaders[TAG_TRUE] = function()
	return true
end
-- Readers of codecs ignore the depth argument.
local function reader(codec: Codec): (depth: number) -> any
	return codec.read :: any
end

autoReaders[TAG_U8] = reader(Serdes.uint8)
autoReaders[TAG_U16] = reader(Serdes.uint16)
autoReaders[TAG_U32] = reader(Serdes.uint32)
autoReaders[TAG_NEG_U8] = function()
	return -Serdes.uint8.read()
end
autoReaders[TAG_NEG_U16] = function()
	return -Serdes.uint16.read()
end
autoReaders[TAG_NEG_U32] = function()
	return -Serdes.uint32.read()
end
autoReaders[TAG_F32] = reader(Serdes.float32)
autoReaders[TAG_F64] = reader(Serdes.float64)
autoReaders[TAG_STRING] = reader(Serdes.string)
autoReaders[TAG_VECTOR2] = reader(Serdes.vec2)
autoReaders[TAG_VECTOR3] = reader(Serdes.vec3)
autoReaders[TAG_COLOR3] = reader(Serdes.color3)
autoReaders[TAG_CFRAME] = reader(Serdes.cframe)
autoReaders[TAG_BUFFER] = reader(Serdes.buff)
autoReaders[TAG_INSTANCE] = reader(Serdes.inst)
-- Types `auto` sends with tags of their own. From a client, a ref holding one
-- of them (or nothing) wasn't written by `auto` and is refused, so a table
-- can't get past auto's depth limit this way.
local AUTO_OWN_TYPES = {
	["nil"] = true,
	boolean = true,
	number = true,
	string = true,
	table = true,
	buffer = true,
	Vector2 = true,
	Vector3 = true,
	CFrame = true,
	Color3 = true,
	Instance = true,
}
autoReaders[TAG_REF] = function()
	rrefIndex += 1
	local refs = rrefs
	local value = if refs then refs[rrefIndex] else nil
	if not rtrusted and AUTO_OWN_TYPES[typeof(value)] then
		error("auto: unexpected ref", 0)
	end
	return value
end
autoReaders[TAG_ARRAY] = function(depth: number)
	if depth >= AUTO_MAX_DEPTH then
		error("auto: nested too deeply", 0)
	end
	local count = readVarUInt()
	checkCount(count, 1)
	local result = table.create(count)
	for index = 1, count do
		result[index] = autoRead(depth + 1)
	end
	return result
end
autoReaders[TAG_DICTIONARY] = function(depth: number)
	if depth >= AUTO_MAX_DEPTH then
		error("auto: nested too deeply", 0)
	end
	local count = readVarUInt()
	checkCount(count, 2)
	local result = {}
	for _ = 1, count do
		local k = autoRead(depth + 1)
		local v = autoRead(depth + 1)
		if k ~= nil and k == k then
			result[k] = v
		end
	end
	return result
end

Serdes.auto = {
	kind = "var",
	desc = "auto",
	minSize = 1,
	write = function(value: any)
		autoWrite(value, 0)
	end,
	read = function(): any
		return autoRead(0)
	end,
} :: Codec

return Serdes
