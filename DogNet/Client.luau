--!strict
--!optimize 2

--[[
	Client side of the transport. Outgoing messages are batched per frame.

	Incoming reliable data can reference a namespace this client hasn't
	required yet (common while a client is still loading). Instead of failing,
	reading stops at that message and everything from there on is held, in
	order, until the namespace is defined.

	The reliable stream each way remembers dictionary key sets (see Serdes),
	with the server's KEY_SET_MEMORY, which it publishes on its remote.
]]

local RunService = game:GetService("RunService")

local Config = require(script.Parent.Config)
local Registry = require(script.Parent.Registry)
local Serdes = require(script.Parent.Serdes)

type Write = (any) -> ()
type Held = {
	batch: buffer,
	refs: { any }?,
	cursor: number,
	refIndex: number,
	id: number,
}

local IS_STUDIO = RunService:IsStudio()
local MAX_BATCH_SIZE = Config.maxBatchSize
local UNRELIABLE_LIMIT = Config.unreliableLimit
local HOLD_WARNING_DELAY = 5

local reliable = Serdes.channel()
local unreliable = Serdes.channel()
local scratch = Serdes.channel()

local reliableRemote: RemoteEvent = nil :: any
local unreliableRemote: UnreliableRemoteEvent = nil :: any
-- Key sets the server's stream to this client remembers.
local readShapes: Serdes.Shapes? = nil

local held: { Held } = {}
local holdGeneration = 0
local warnedOversize: { [number]: boolean } = {}
local warnedBatchSize = false

local Client = {}

-- Sends what `channel` holds and empties it. Refs only when there are any:
-- a trailing nil argument costs a byte on the wire.
local function fire(remote: RemoteEvent | UnreliableRemoteEvent, channel: Serdes.Channel)
	local batch, refs = Serdes.dump(channel)
	if refs then
		(remote :: any):FireServer(batch, refs)
	else
		(remote :: any):FireServer(batch)
	end
	Serdes.reset(channel)
end

-- Packets write straight into this with their own send function (Serdes.sender).
Client.reliableChannel = reliable

function Client.send(isReliable: boolean, id: number, callId: number?, write: Write, value: any)
	if isReliable then
		Serdes.message(reliable, id, callId, write, value)
		return
	end

	Serdes.reset(scratch)
	Serdes.message(scratch, id, callId, write, value)

	local cost = Serdes.cost(scratch)
	if cost > UNRELIABLE_LIMIT then
		if not warnedOversize[id] then
			warnedOversize[id] = true
			warn(
				`[DogNet] an unreliable message in {Registry.describe(id)} is over the {UNRELIABLE_LIMIT} byte limit Roblox allows; sending it reliably instead`
			)
		end
		Serdes.append(reliable, scratch, 0, scratch.cursor, 0, scratch.refCount)
		return
	end
	if unreliable.cursor > 0 and Serdes.cost(unreliable) + cost > UNRELIABLE_LIMIT then
		fire(unreliableRemote, unreliable)
	end
	Serdes.append(unreliable, scratch, 0, scratch.cursor, 0, scratch.refCount)
end

local function flush()
	if reliable.cursor > 0 then
		if IS_STUDIO and reliable.cursor > MAX_BATCH_SIZE and not warnedBatchSize then
			warnedBatchSize = true
			warn(
				`[DogNet] this client sent {reliable.cursor} bytes in one frame; the server rejects batches over {MAX_BATCH_SIZE} bytes (MAX_BUFFER_SIZE)`
			)
		end
		fire(reliableRemote, reliable)
	end
	if unreliable.cursor > 0 then
		fire(unreliableRemote, unreliable)
	end
end

local function warnIfStillHeld(generation: number)
	if holdGeneration == generation and held[1] then
		warn(
			`[DogNet] received data for {Registry.describe(held[1].id)}, which this client hasn't defined. Reliable data from the server is on hold until the client requires that namespace's module.`
		)
	end
end

local function startHolding()
	holdGeneration += 1
	task.delay(HOLD_WARNING_DELAY, warnIfStillHeld, holdGeneration)
end

--[[
	Reads what's waiting, oldest first: a batch a listener left partway (it
	yielded or errored; see Serdes.readTrusted), then batches held for a
	namespace this client hasn't defined yet. Stops at the first one that
	still can't be read. If a listener stalls a batch again, this doesn't
	return until it resumes, and then stops.
]]
local function readWaiting()
	if Serdes.stalled() then
		local batch, refs, stoppedAt, stoppedRef, badId, failed, taken = Serdes.continueStalled()
		if taken then
			return
		end
		if failed then
			warn("[DogNet] couldn't read data from the server (see the error above); dropped the rest of that batch")
			-- What was dropped may have added key sets: start them over.
			if readShapes and Serdes.lostTrack(readShapes) then
				Serdes.requestReset(reliable)
			end
		elseif stoppedAt then
			table.insert(held, 1, {
				batch = batch,
				refs = refs,
				cursor = stoppedAt,
				refIndex = stoppedRef :: number,
				id = badId :: number,
			})
			startHolding()
			return
		end
	end
	while held[1] do
		local entry = table.remove(held, 1) :: Held
		local stoppedAt, stoppedRef, badId, taken = Serdes.readTrusted(
			entry.batch,
			entry.refs,
			Registry.readers,
			Registry.targets,
			entry.cursor,
			entry.refIndex,
			readShapes
		)
		if taken then
			return
		end
		if stoppedAt then
			entry.cursor = stoppedAt
			entry.refIndex = stoppedRef :: number
			entry.id = badId :: number
			table.insert(held, 1, entry)
			startHolding()
			return
		end
	end
	holdGeneration += 1
end

local function drain()
	readWaiting()
	Serdes.setWaiting(held[1] ~= nil)
end

-- A batch that arrived while others were waiting or stalled: older data goes first.
local function queue(batch: buffer, refs: { any }?)
	table.insert(held, { batch = batch, refs = refs, cursor = 0, refIndex = 0, id = 0 })
	Serdes.setWaiting(true)
	if Serdes.stalled() then
		drain()
	end
end

-- A batch that reached data for a namespace this client hasn't defined yet.
local function hold(batch: buffer, refs: { any }?, cursor: number, refIndex: number, id: number)
	table.insert(held, { batch = batch, refs = refs, cursor = cursor, refIndex = refIndex, id = id })
	Serdes.setWaiting(true)
	startHolding()
end

local function receiveUnreliable(batch: any, refs: any)
	if typeof(batch) ~= "buffer" then
		return
	end
	-- Anything for a namespace this client hasn't defined is simply dropped:
	-- it's unreliable data, and holding it would only delay newer updates.
	local _, _, _, failure = Serdes.process(batch, refs, nil, Registry.readers, Registry.targets, true, 0, 0, nil)
	if failure ~= nil then
		task.spawn(error, `[DogNet] couldn't read data from the server: {failure}`, 0)
	end
end

function Client.start(root: Instance)
	reliableRemote = root:WaitForChild("Reliable") :: RemoteEvent
	unreliableRemote = root:WaitForChild("Unreliable") :: UnreliableRemoteEvent

	-- Both ends must use the same budget, so the server's.
	local memory = reliableRemote:GetAttribute("KeySetMemory")
	if type(memory) == "number" and memory > 0 then
		Serdes.remember(reliable, memory)
		readShapes = Serdes.shapes(memory)
	end
	-- The server lost track of the key sets this client sends: start over.
	Registry.readers[Serdes.CONTROL_ID] = Serdes.controlReader(function()
		Serdes.resetShapes(reliable)
	end)

	local receiveReliable = Serdes.trustedReceiver(Registry.readers, Registry.targets, queue, hold, readShapes)
	reliableRemote.OnClientEvent:Connect(receiveReliable)
	unreliableRemote.OnClientEvent:Connect(receiveUnreliable)

	Registry.onDefined(function()
		if held[1] then
			task.defer(drain)
		end
	end)

	RunService.Heartbeat:Connect(function()
		task.defer(flush)
		-- A listener stalled a batch earlier this frame: read the rest now.
		if Serdes.stalled() then
			drain()
		end
	end)
end

return Client
