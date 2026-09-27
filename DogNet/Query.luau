--!strict
--!optimize 2

--[[
	Request/response over the same batched remotes as packets, in both
	directions (client asks server, or server asks one client). No
	RemoteFunction: a request carries a call id, the response comes back with
	it, and the waiting thread is resumed directly. Invocations time out, so a
	client that never answers can't hang the server.

	Call ids go on the wire as varints and are reused smallest first, so with
	fewer than 128 calls waiting they take one byte. The id of a call that
	timed out stays unused for a while, in case its answer still comes.

	Each query uses two ids: requests on `id`, responses on `id + 1`. Failures
	for any query share one id and carry a reason code.
]]

local RunService = game:GetService("RunService")

local Client = require(script.Parent.Client)
local Config = require(script.Parent.Config)
local Registry = require(script.Parent.Registry)
local Runner = require(script.Parent.Runner)
local Serdes = require(script.Parent.Serdes)
local Server = require(script.Parent.Server)

type Write = (any) -> ()
type Handler = (request: any, player: Player?) -> any
type Call = {
	thread: thread,
	player: Player?,
	responseId: number,
	-- os.clock() time it fails at, and its timeout in seconds (for the message).
	deadline: number,
	seconds: number,
}

local IS_SERVER = RunService:IsServer()
local ERROR_ID = Registry.QUERY_ERROR_ID
local MAX_IN_FLIGHT = Config.maxQueriesInFlight
-- Client only: requests kept until a handler is set.
local BACKLOG_LIMIT = 64

local FAILED_HANDLER_ERRORED = 0
local FAILED_NO_HANDLER = 1
local FAILED_BUSY = 2
local FAILED_BAD_RESPONSE = 3
local FAILURE_REASONS: { [number]: string } = {
	[FAILED_HANDLER_ERRORED] = "its handler errored",
	[FAILED_NO_HANDLER] = "nothing is handling it",
	[FAILED_BUSY] = "too many requests are already in flight",
	[FAILED_BAD_RESPONSE] = "its handler returned a value that doesn't match the response type",
}

-- How often pending calls are checked for timeouts. A timer per call
-- (task.delay, then task.cancel on the answer) cost more than the rest of
-- an invoke; one sweep of what's pending is nearly free.
local SWEEP_INTERVAL = 0.25
-- Seconds the id of a call that timed out stays unused.
local QUARANTINE = 60
local MAX_CALLS = 65536

local pending: { [number]: Call } = {}
local pendingCount = 0
local nextSweep = 0
-- Ids free for reuse (a stack, so the smallest recent ones come back first),
-- and the next never-used one.
local freeIds: { number } = {}
local freeCount = 0
local freshId = 0
-- Ids of calls that timed out, and when each can be reused.
local quarantined: { [number]: number } = {}
local quarantineCount = 0

local function traceback(message: any): string
	return debug.traceback(tostring(message), 2)
end

-- The id the next call gets. Taken with `takeCallId` once its request is
-- written, so a request that fails to write leaves nothing to give back.
local function nextCallId(): number
	if freeCount > 0 then
		return freeIds[freeCount]
	end
	if freshId >= MAX_CALLS then
		error("[DogNet] too many queries in flight", 3)
	end
	return freshId
end

local function takeCallId()
	if freeCount > 0 then
		freeIds[freeCount] = nil
		freeCount -= 1
	else
		freshId += 1
	end
end

local function releaseCallId(callId: number)
	freeCount += 1
	freeIds[freeCount] = callId
end

local function settle(callId: number, sender: Player?, responseId: number?, ok: boolean, value: any)
	local call = pending[callId]
	-- A response must come from whoever was asked, for the query that was asked.
	if call == nil or call.player ~= sender or (responseId ~= nil and call.responseId ~= responseId) then
		return
	end
	pending[callId] = nil
	pendingCount -= 1
	releaseCallId(callId)
	task.spawn(call.thread, ok, value)
end

local function sweep()
	if pendingCount == 0 and quarantineCount == 0 then
		return
	end
	local now = os.clock()
	if now < nextSweep then
		return
	end
	nextSweep = now + SWEEP_INTERVAL
	if quarantineCount > 0 then
		for callId, reuseAt in quarantined do
			if reuseAt <= now then
				quarantined[callId] = nil
				quarantineCount -= 1
				releaseCallId(callId)
			end
		end
	end
	-- Collected first: resuming a caller can start new calls, which must not
	-- happen while `pending` is being traversed.
	local expired: { Call }? = nil
	for callId, call in pending do
		if call.deadline <= now then
			pending[callId] = nil
			pendingCount -= 1
			quarantined[callId] = now + QUARANTINE
			quarantineCount += 1
			expired = expired or {}
			table.insert(expired :: { Call }, call)
		end
	end
	if expired then
		for _, call in expired do
			task.spawn(call.thread, false, `no response within {call.seconds} seconds`)
		end
	end
end

RunService.Heartbeat:Connect(sweep)

local function send(target: Player?, id: number, callId: number, write: Write, value: any)
	if IS_SERVER then
		Server.sendTo(target :: Player, true, id, callId, write, value)
	else
		Client.send(true, id, callId, write, value)
	end
end

local function fail(target: Player?, callId: number, reason: number)
	send(target, ERROR_ID, callId, Serdes.uint8.write, reason)
end

Registry.readers[ERROR_ID] = function(sender: Player?): nil
	local callId = Serdes.readVarUInt()
	local reason = Serdes.readU8()
	settle(callId, sender, nil, false, FAILURE_REASONS[reason] or "the other side reported an error")
	return nil
end

if IS_SERVER then
	Server.onPlayerRemoving(function(player: Player)
		local orphaned: { Call } = {}
		for callId, call in pending do
			if call.player == player then
				pending[callId] = nil
				pendingCount -= 1
				-- Nothing more can come from them.
				releaseCallId(callId)
				table.insert(orphaned, call)
			end
		end
		for _, call in orphaned do
			task.spawn(call.thread, false, "the player left")
		end
	end)
end

local Query = {}

function Query.create(
	label: string,
	id: number,
	requestCodec: Serdes.Codec,
	responseCodec: Serdes.Codec,
	timeout: number
)
	local responseId = id + 1
	local writeRequest, readRequest = requestCodec.write, requestCodec.read
	local writeResponse, readResponse = responseCodec.write, responseCodec.read

	local handler: Handler? = nil
	local backlog: { { callId: number, value: any } } = {}

	local function run(callback: Handler, sender: Player?, callId: number, value: any)
		local ok, result = xpcall(callback, traceback, value, sender)
		if IS_SERVER then
			local state = Server.state(sender :: Player)
			if state then
				state.inflight -= 1
			end
		end

		if not ok then
			task.spawn(error, `[DogNet] {label} handler errored: {result}`, 0)
			fail(sender, callId, FAILED_HANDLER_ERRORED)
			return
		end
		local sent, problem = pcall(send :: (...any) -> ...any, sender, responseId, callId, writeResponse, result)
		if not sent then
			task.spawn(
				error,
				`[DogNet] {label} handler returned a value that can't be sent as its response: {problem}`,
				0
			)
			fail(sender, callId, FAILED_BAD_RESPONSE)
		end
	end

	-- Queries act on what they read, so they have no listeners.
	Registry.readers[id] = function(sender: Player?): nil
		local callId = Serdes.readVarUInt()
		local value = readRequest()

		local callback = handler
		if callback == nil then
			if not IS_SERVER and #backlog < BACKLOG_LIMIT then
				table.insert(backlog, { callId = callId, value = value })
			else
				fail(sender, callId, FAILED_NO_HANDLER)
			end
			return nil
		end

		if IS_SERVER then
			local state = Server.state(sender :: Player)
			if state == nil then
				return nil
			end
			if state.inflight >= MAX_IN_FLIGHT then
				fail(sender, callId, FAILED_BUSY)
				return nil
			end
			state.inflight += 1
			-- Right here, on the thread reading the batch, like packet listeners:
			-- if the handler yields, Serdes.process carries on with the rest of
			-- the batch on a new reader, and this one answers when it resumes.
			run(callback, sender, callId, value)
			return nil
		end
		-- The client reads the server's batches on the calling thread, which a
		-- yielding handler would hold up, so handlers run on their own.
		Runner(run, callback, sender, callId, value)
		return nil
	end

	Registry.readers[responseId] = function(sender: Player?): nil
		local callId = Serdes.readVarUInt()
		local value = readResponse()
		settle(callId, sender, responseId, true, value)
		return nil
	end

	local query = {}

	function query.invoke(request: any, player: Player?): any
		if IS_SERVER then
			if typeof(player) ~= "Instance" then
				error(`[DogNet] {label}.invoke on the server needs the player to ask: invoke(request, player)`, 2)
			elseif Server.state(player :: Player) == nil then
				error(`[DogNet] {label}.invoke: that player isn't in the game`, 2)
			end
		end

		-- Write first: if the request doesn't match its type, nothing is left pending.
		local callId = nextCallId()
		send(player, id, callId, writeRequest, request)
		takeCallId()

		pending[callId] = {
			thread = coroutine.running(),
			player = if IS_SERVER then player else nil,
			responseId = responseId,
			deadline = os.clock() + timeout,
			seconds = timeout,
		}
		pendingCount += 1

		local ok, result = coroutine.yield()
		if not ok then
			error(`[DogNet] {label}.invoke failed: {result}`, 2)
		end
		return result
	end

	function query.listen(callback: Handler): () -> ()
		if type(callback) ~= "function" then
			error(`[DogNet] {label}: listen expects a function`, 2)
		end
		if handler ~= nil and handler ~= callback then
			warn(
				`[DogNet] {label} already had a handler and this one replaces it (a query has one handler, and its return value is the response)`
			)
		end
		handler = callback

		if #backlog > 0 then
			local queued = backlog
			backlog = {}
			task.defer(function()
				for _, item in queued do
					local current = handler
					if current then
						Runner(run, current, nil, item.callId, item.value)
					else
						fail(nil, item.callId, FAILED_NO_HANDLER)
					end
				end
			end)
		end

		return function()
			if handler == callback then
				handler = nil
			end
		end
	end

	function query.disconnectAll()
		handler = nil
	end

	return query
end

return Query
