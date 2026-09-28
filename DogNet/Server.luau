--!strict
--!optimize 2

--[[
	Server side of the transport. Messages are batched per frame and fired once
	per remote per player, after every Heartbeat handler has had its chance to
	send.

	Ordering between sendToAll and targeted sends is preserved: broadcasts go
	into one shared channel, and the first time a player gets a targeted message
	in a frame, the broadcast bytes queued so far are spliced into that player's
	channel ahead of it. Players with nothing targeted get the shared batch as
	is, via a single FireAllClients when nobody has anything targeted.

	Each player's reliable stream remembers dictionary key sets, both ways
	(see Serdes): what the server sends that player only (sendTo, query
	traffic) and what the player sends. Broadcasts don't: a player who joins
	later wouldn't have them.
]]

local Players = game:GetService("Players")
local RunService = game:GetService("RunService")

local Config = require(script.Parent.Config)
local Registry = require(script.Parent.Registry)
local Serdes = require(script.Parent.Serdes)

type Channel = Serdes.Channel
type Write = (any) -> ()

export type PlayerState = {
	player: Player,
	reliable: Channel,
	unreliable: Channel,
	-- Key sets this player's stream to the server remembers.
	readShapes: Serdes.Shapes?,
	-- How much of this frame's broadcast channel is already spliced into `reliable`.
	synced: number,
	syncedRefs: number,
	reliableQueued: boolean,
	unreliableQueued: boolean,
	-- Inbound rate limit: a byte budget that refills over time.
	budget: number,
	refilledAt: number,
	-- Query requests from this player whose handlers are still running.
	inflight: number,
	warnedAt: number,
}

local IS_STUDIO = RunService:IsStudio()
local MAX_BATCH_SIZE = Config.maxBatchSize
local RATE_LIMIT = Config.rateLimit
local UNRELIABLE_LIMIT = Config.unreliableLimit
local KEY_SET_MEMORY = Config.keySetMemory

local states: { [Player]: PlayerState } = {}
-- Players who left, so late sends (e.g. from a PlayerRemoving handler) don't recreate their state.
local departed: { [Player]: boolean } = setmetatable({}, { __mode = "k" }) :: any
local players: { Player } = {}
local reliableQueue: { PlayerState } = {}
local unreliableQueue: { PlayerState } = {}
local leaveCallbacks: { (Player) -> () } = {}
local warnedOversize: { [number]: boolean } = {}

local broadcast = Serdes.channel()
local broadcastUnreliable = Serdes.channel()
-- Holds one encoded message that gets copied into several channels.
local scratch = Serdes.channel()

local reliableRemote: RemoteEvent = nil :: any
local unreliableRemote: UnreliableRemoteEvent = nil :: any

local Server = {}

-- Sends what `channel` holds to `player`, or to everyone, without emptying
-- it. Refs only when there are any: a trailing nil argument costs a byte.
local function fire(remote: RemoteEvent | UnreliableRemoteEvent, channel: Channel, player: Player?)
	local batch, refs = Serdes.dump(channel)
	local target = remote :: any
	if player then
		if refs then
			target:FireClient(player, batch, refs)
		else
			target:FireClient(player, batch)
		end
	elseif refs then
		target:FireAllClients(batch, refs)
	else
		target:FireAllClients(batch)
	end
end

-- Reliable packets broadcast straight into this, with their own send function (Serdes.sender).
Server.broadcast = broadcast

local function getState(player: Player): PlayerState?
	local state = states[player]
	if state == nil then
		if typeof(player) ~= "Instance" or departed[player] or player.Parent ~= Players then
			return nil
		end
		local reliable = Serdes.channel()
		local readShapes: Serdes.Shapes? = nil
		if KEY_SET_MEMORY > 0 then
			Serdes.remember(reliable, KEY_SET_MEMORY)
			readShapes = Serdes.shapes(KEY_SET_MEMORY)
		end
		state = {
			player = player,
			reliable = reliable,
			unreliable = Serdes.channel(),
			readShapes = readShapes,
			synced = 0,
			syncedRefs = 0,
			reliableQueued = false,
			unreliableQueued = false,
			budget = MAX_BATCH_SIZE,
			refilledAt = os.clock(),
			inflight = 0,
			warnedAt = -math.huge,
		}
		states[player] = state
	end
	return state
end

Server.state = getState

local function syncBroadcast(state: PlayerState)
	local cursor = broadcast.cursor
	if cursor > state.synced then
		Serdes.append(state.reliable, broadcast, state.synced, cursor, state.syncedRefs, broadcast.refCount)
		state.synced = cursor
		state.syncedRefs = broadcast.refCount
	end
end

local function queueReliable(state: PlayerState)
	if not state.reliableQueued then
		state.reliableQueued = true
		table.insert(reliableQueue, state)
	end
end

local function encode(id: number, callId: number?, write: Write, value: any)
	Serdes.reset(scratch)
	Serdes.message(scratch, id, callId, write, value)
end

local function warnOversize(id: number)
	if not warnedOversize[id] then
		warnedOversize[id] = true
		warn(
			`[DogNet] an unreliable message in {Registry.describe(id)} is over the {UNRELIABLE_LIMIT} byte limit Roblox allows; sending it reliably instead`
		)
	end
end

-- Adds the message in `scratch` to an unreliable channel, first firing what the
-- channel holds if both together would go over the limit. Returns false when
-- the message is too big to ever go unreliably.
local function pushUnreliable(channel: Channel, player: Player?): boolean
	local cost = Serdes.cost(scratch)
	if cost > UNRELIABLE_LIMIT then
		return false
	end
	if channel.cursor > 0 and Serdes.cost(channel) + cost > UNRELIABLE_LIMIT then
		fire(unreliableRemote, channel, player)
		Serdes.reset(channel)
	end
	Serdes.append(channel, scratch, 0, scratch.cursor, 0, scratch.refCount)
	return true
end

local function pushReliable(state: PlayerState)
	syncBroadcast(state)
	Serdes.append(state.reliable, scratch, 0, scratch.cursor, 0, scratch.refCount)
	queueReliable(state)
end

-- Sends the message in `scratch` to one player.
local function pushTo(state: PlayerState, reliable: boolean, id: number)
	if reliable then
		pushReliable(state)
	elseif pushUnreliable(state.unreliable, state.player) then
		if not state.unreliableQueued then
			state.unreliableQueued = true
			table.insert(unreliableQueue, state)
		end
	else
		warnOversize(id)
		pushReliable(state)
	end
end

function Server.sendTo(player: Player, reliable: boolean, id: number, callId: number?, write: Write, value: any)
	local state = getState(player)
	if state == nil then
		return
	end

	if reliable then
		-- Common case: write straight into the player's channel. Queued first,
		-- so the broadcast bytes spliced in go out even if the write errors.
		syncBroadcast(state)
		queueReliable(state)
		Serdes.message(state.reliable, id, callId, write, value)
	else
		encode(id, callId, write, value)
		pushTo(state, false, id)
	end
end

function Server.sendToAll(reliable: boolean, id: number, callId: number?, write: Write, value: any)
	if reliable then
		Serdes.message(broadcast, id, callId, write, value)
		return
	end

	encode(id, callId, write, value)
	if not pushUnreliable(broadcastUnreliable, nil) then
		warnOversize(id)
		Serdes.append(broadcast, scratch, 0, scratch.cursor, 0, scratch.refCount)
	end
end

function Server.sendToList(
	targets: { Player },
	reliable: boolean,
	id: number,
	callId: number?,
	write: Write,
	value: any
)
	encode(id, callId, write, value)
	for _, player in targets do
		local state = getState(player)
		if state then
			pushTo(state, reliable, id)
		end
	end
end

function Server.sendToAllExcept(
	except: Player?,
	reliable: boolean,
	id: number,
	callId: number?,
	write: Write,
	value: any
)
	encode(id, callId, write, value)
	for _, player in players do
		if player ~= except then
			local state = getState(player)
			if state then
				pushTo(state, reliable, id)
			end
		end
	end
end

-- Called when a player leaves, e.g. to fail queries waiting on them.
function Server.onPlayerRemoving(callback: (Player) -> ())
	table.insert(leaveCallbacks, callback)
end

local function flush()
	if #reliableQueue == 0 then
		if broadcast.cursor > 0 then
			fire(reliableRemote, broadcast, nil)
			-- Everyone read its array counts (see Serdes.shareCounts).
			if broadcast.countKeys then
				for _, state in states do
					Serdes.shareCounts(state.reliable, broadcast)
				end
			end
		end
	else
		if broadcast.cursor > 0 then
			local batch, refs = Serdes.dump(broadcast)
			for _, player in players do
				local state = states[player]
				if state == nil or not state.reliableQueued then
					if refs then
						reliableRemote:FireClient(player, batch, refs)
					else
						reliableRemote:FireClient(player, batch)
					end
					if state and broadcast.countKeys then
						Serdes.shareCounts(state.reliable, broadcast)
					end
				end
			end
		end
		for _, state in reliableQueue do
			if states[state.player] == state then
				syncBroadcast(state)
				fire(reliableRemote, state.reliable, state.player)
			end
			Serdes.reset(state.reliable)
			state.synced = 0
			state.syncedRefs = 0
			state.reliableQueued = false
		end
		table.clear(reliableQueue)
	end
	Serdes.reset(broadcast)

	if broadcastUnreliable.cursor > 0 then
		fire(unreliableRemote, broadcastUnreliable, nil)
		Serdes.reset(broadcastUnreliable)
	end
	for _, state in unreliableQueue do
		if states[state.player] == state and state.unreliable.cursor > 0 then
			fire(unreliableRemote, state.unreliable, state.player)
		end
		Serdes.reset(state.unreliable)
		state.unreliableQueued = false
	end
	table.clear(unreliableQueue)
end

-- Part of this player's reliable stream was dropped, maybe with key sets it
-- added: stop trusting references and ask the client to start them over.
local function lostTrack(state: PlayerState)
	local shapes = state.readShapes
	if shapes and Serdes.lostTrack(shapes) then
		Serdes.requestReset(state.reliable)
		queueReliable(state)
	end
end

local function reject(state: PlayerState, reason: string)
	-- Throttled so a misbehaving client can't flood the output.
	local now = os.clock()
	if now - state.warnedAt >= (if IS_STUDIO then 1 else 30) then
		state.warnedAt = now
		warn(`[DogNet] dropped data from {state.player.Name}: {reason}`)
	end
end

local function receive(player: Player, batch: any, refs: any, reliable: boolean): ()
	local state = getState(player)
	if state == nil then
		return
	end
	local problem: string? = nil
	if typeof(batch) ~= "buffer" then
		problem = "expected a buffer"
	elseif refs ~= nil and type(refs) ~= "table" then
		problem = "malformed refs"
	else
		local length = buffer.len(batch)
		local now = os.clock()
		state.budget = math.min(MAX_BATCH_SIZE, state.budget + (now - state.refilledAt) * RATE_LIMIT)
		state.refilledAt = now
		if length > MAX_BATCH_SIZE then
			problem = `batch of {length} bytes is over the {MAX_BATCH_SIZE} byte limit`
		elseif length > state.budget then
			problem = "rate limit exceeded"
		else
			state.budget -= length
			-- Refs count too: Roblox sent them along, and `unknown` can carry anything.
			local refsCost = if refs == nil then 0 else Serdes.refsCost(refs, state.budget)
			if refsCost == nil then
				problem = "malformed refs"
				state.budget = 0
			elseif refsCost > state.budget then
				problem = "rate limit exceeded"
				-- Nothing more from this player is read until the budget refills.
				state.budget = 0
			else
				state.budget -= refsCost
				-- Zero-byte items (Instances in an array, say) cost a byte each,
				-- out of what's left.
				local stoppedAt, _, badId, failure, zeroItems = Serdes.process(
					batch,
					refs,
					player,
					Registry.readers,
					Registry.targets,
					false,
					0,
					0,
					if reliable then state.readShapes else nil,
					state.budget
				)
				state.budget -= zeroItems
				if failure ~= nil then
					problem = `malformed batch ({failure})`
				elseif stoppedAt then
					problem = `unknown message id {badId}`
				end
			end
		end
	end
	if problem then
		reject(state, problem)
		if reliable then
			lostTrack(state)
		end
	end
end

function Server.start(root: Instance)
	local reliable = Instance.new("RemoteEvent")
	reliable.Name = "Reliable"
	-- Clients remember key sets with the server's budget (see Client.start).
	reliable:SetAttribute("KeySetMemory", KEY_SET_MEMORY)
	reliable.Parent = root
	reliableRemote = reliable

	local unreliable = Instance.new("UnreliableRemoteEvent")
	unreliable.Name = "Unreliable"
	unreliable.Parent = root
	unreliableRemote = unreliable

	reliable.OnServerEvent:Connect(function(player: Player, batch: any, refs: any)
		receive(player, batch, refs, true)
	end)
	unreliable.OnServerEvent:Connect(function(player: Player, batch: any, refs: any)
		receive(player, batch, refs, false)
	end)

	-- A client lost track of the key sets sent to it: start them over.
	Registry.readers[Serdes.CONTROL_ID] = Serdes.controlReader(function(player: Player?)
		local state = player and states[player]
		if state then
			Serdes.resetShapes(state.reliable)
			queueReliable(state)
		end
	end)

	local function onPlayerAdded(player: Player)
		if not table.find(players, player) then
			table.insert(players, player)
		end
	end
	Players.PlayerAdded:Connect(onPlayerAdded)
	for _, player in Players:GetPlayers() do
		onPlayerAdded(player)
	end

	Players.PlayerRemoving:Connect(function(player: Player)
		departed[player] = true
		states[player] = nil
		local index = table.find(players, player)
		if index then
			table.remove(players, index)
		end
		for _, callback in leaveCallbacks do
			callback(player)
		end
	end)

	-- Deferring runs the flush after the rest of this frame's Heartbeat
	-- handlers, so what they send still goes out this frame.
	RunService.Heartbeat:Connect(function()
		task.defer(flush)
	end)
end

return Server
