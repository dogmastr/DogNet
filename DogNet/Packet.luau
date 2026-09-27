--!strict
--!optimize 2

local RunService = game:GetService("RunService")

local Client = require(script.Parent.Client)
local Registry = require(script.Parent.Registry)
local Runner = require(script.Parent.Runner)
local Serdes = require(script.Parent.Serdes)
local Server = require(script.Parent.Server)

type Callback = (value: any, player: Player?) -> ()

local IS_SERVER = RunService:IsServer()
-- Client only: messages that arrive before a packet's first listener connects
-- are kept, so a listener that connects a little late (a UI that loads after
-- the first update) still gets them. After that, as usual, messages with no
-- listener are dropped.
local BACKLOG_LIMIT = 256

return function(label: string, id: number, codec: Serdes.Codec, reliable: boolean)
	local write = codec.write
	local read = codec.read
	local readers = Registry.readers
	local targets = Registry.targets

	-- Replaced, never mutated, so dispatch can iterate without copying.
	local listeners: { Callback } = {}

	local backlog: { any } = {}
	local backlogHead, backlogTail = 1, 0
	local holding = false
	local listenedBefore = false
	local drainScheduled = false
	local warnedBacklog = false

	local packet = {}

	-- Client only, until the first listener connects and anything kept has
	-- been handed over: read each message into the backlog instead.
	local function hold(): nil
		local value = read()
		if backlogTail - backlogHead + 1 >= BACKLOG_LIMIT then
			if not warnedBacklog then
				warnedBacklog = true
				warn(
					`[DogNet] {label} got more than {BACKLOG_LIMIT} messages before anything listened; dropping the extras`
				)
			end
			return nil
		end
		holding = true
		backlogTail += 1
		backlog[backlogTail] = value
		return nil
	end

	-- Points this id at the backlog or at the listeners. Called whenever
	-- either changes, so reading a message never has to check.
	local function route()
		if not IS_SERVER and (holding or not listenedBefore) then
			readers[id] = hold
			targets[id] = nil
		else
			readers[id] = read
			targets[id] = listeners
		end
	end
	route()

	local function dispatch(value: any, sender: Player?)
		local current = listeners
		for index = 1, #current do
			Runner(current[index], value, sender)
		end
	end

	local function drain()
		drainScheduled = false
		while backlogHead <= backlogTail and #listeners > 0 do
			local value = backlog[backlogHead]
			backlog[backlogHead] = nil
			backlogHead += 1
			dispatch(value, nil)
		end
		if backlogHead > backlogTail then
			backlogHead, backlogTail = 1, 0
			holding = false
			route()
		end
	end

	local function connect(callback: Callback): () -> ()
		if type(callback) ~= "function" then
			error(`[DogNet] {label}: listen expects a function`, 3)
		end
		local updated = table.clone(listeners)
		table.insert(updated, callback)
		listeners = updated
		listenedBefore = true
		route()

		if holding and not drainScheduled then
			drainScheduled = true
			task.defer(drain)
		end

		local connected = true
		return function()
			if not connected then
				return
			end
			connected = false
			local remaining = table.clone(listeners)
			local index = table.find(remaining, callback)
			if index then
				table.remove(remaining, index)
			end
			listeners = remaining
			route()
		end
	end

	function packet.listen(callback: Callback): () -> ()
		return connect(callback)
	end

	function packet.listenOnce(callback: Callback): () -> ()
		if type(callback) ~= "function" then
			error(`[DogNet] {label}: listenOnce expects a function`, 2)
		end
		local disconnect: () -> ()
		disconnect = connect(function(value: any, sender: Player?)
			disconnect()
			callback(value, sender)
		end)
		return disconnect
	end

	function packet.wait(): (any, Player?)
		local thread = coroutine.running()
		packet.listenOnce(function(value: any, sender: Player?)
			task.spawn(thread, value, sender)
		end)
		return coroutine.yield()
	end

	function packet.disconnectAll()
		listeners = {}
		route()
	end

	if IS_SERVER then
		function packet.sendTo(data: any, player: Player)
			if typeof(player) ~= "Instance" then
				error(`[DogNet] {label}.sendTo(data, player) needs a Player as its second argument`, 2)
			end
			Server.sendTo(player, reliable, id, nil, write, data)
		end

		if reliable then
			packet.sendToAll = Serdes.sender(Server.broadcast, id, codec)
		else
			function packet.sendToAll(data: any)
				Server.sendToAll(false, id, nil, write, data)
			end
		end

		function packet.sendToList(data: any, players: { Player })
			Server.sendToList(players, reliable, id, nil, write, data)
		end

		function packet.sendToAllExcept(data: any, except: Player)
			Server.sendToAllExcept(except, reliable, id, nil, write, data)
		end

		function packet.send()
			error(
				`[DogNet] {label}.send is client-only; on the server use sendTo, sendToAll, sendToList or sendToAllExcept`,
				2
			)
		end
	else
		if reliable then
			packet.send = Serdes.sender(Client.reliableChannel, id, codec)
		else
			function packet.send(data: any)
				Client.send(false, id, nil, write, data)
			end
		end

		-- Takes any arguments so it can stand in for every server-only method.
		-- selene: allow(unused_variable)
		local function serverOnly(...: any): ()
			error(
				`[DogNet] {label}: sendTo, sendToAll, sendToList and sendToAllExcept are server-only; use send on the client`,
				2
			)
		end
		packet.sendTo = serverOnly
		packet.sendToAll = serverOnly
		packet.sendToList = serverOnly
		packet.sendToAllExcept = serverOnly
	end

	return packet
end
