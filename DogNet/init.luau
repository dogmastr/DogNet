--!strict

--[[
	DogNet: typed, buffer-based networking for Roblox, rebuilt from ByteNet Max.

	Define a namespace in one ModuleScript that both the server and clients require:

		local DogNet = require(ReplicatedStorage.DogNet)

		return DogNet.defineNamespace("Game", function()
			return {
				packets = {
					Damage = DogNet.definePacket({
						value = DogNet.struct({ target = DogNet.inst, amount = DogNet.uint16 }),
					}),
				},
				queries = {
					GetCoins = DogNet.defineQuery({
						request = DogNet.nothing,
						response = DogNet.uint32,
					}),
				},
			}
		end)

	Documentation: README.md in this project.
]]

local RunService = game:GetService("RunService")

local Client = require(script.Client)
local Config = require(script.Config)
local Packet = require(script.Packet)
local Query = require(script.Query)
local Registry = require(script.Registry)
local Serdes = require(script.Serdes)
local Server = require(script.Server)
local Types = require(script.Types)

export type Packet<T> = Types.Packet<T>
export type Query<Request, Response> = Types.Query<Request, Response>
export type PacketProps<T> = Types.PacketProps<T>
export type QueryProps<Request, Response> = Types.QueryProps<Request, Response>

type Codec = Serdes.Codec

Registry.start(script)
if RunService:IsServer() then
	Server.start(script)
else
	Client.start(script)
end

-- What definePacket and defineQuery return: a description that defineNamespace
-- turns into the real packet or query.
local definitionMeta = {
	__index = function(_, key)
		error(
			`[DogNet] can't use "{key}" on a definition; use the packet or query from the table defineNamespace returns`,
			2
		)
	end,
}

local function isCodec(value: any): boolean
	if type(value) ~= "table" then
		return false
	end
	return type(value.read) == "function" and type(value.write) == "function"
end

local function definePacket(props: any)
	if type(props) ~= "table" or not isCodec(props.value) then
		error(
			'[DogNet] definePacket expects { value = <DogNet data type>, reliabilityType = "reliable" | "unreliable"? }',
			2
		)
	end
	local reliability = props.reliabilityType or "reliable"
	if reliability ~= "reliable" and reliability ~= "unreliable" then
		error(`[DogNet] reliabilityType must be "reliable" or "unreliable", got {reliability}`, 2)
	end
	return setmetatable({
		kind = "packet",
		codec = props.value,
		reliable = reliability == "reliable",
	}, definitionMeta)
end

local function defineQuery(props: any)
	if type(props) ~= "table" or not isCodec(props.request) or not isCodec(props.response) then
		error("[DogNet] defineQuery expects { request = <data type>, response = <data type>, timeout = number? }", 2)
	end
	if props.timeout ~= nil and (type(props.timeout) ~= "number" or props.timeout <= 0) then
		error("[DogNet] a query's timeout must be a positive number of seconds", 2)
	end
	return setmetatable({
		kind = "query",
		request = props.request,
		response = props.response,
		timeout = props.timeout or Config.queryTimeout,
	}, definitionMeta)
end

local function sortedNames(definitions: any, kind: string, namespace: string): { string }
	if type(definitions) ~= "table" then
		error(`[DogNet] namespace "{namespace}": {kind}s must be a table`, 4)
	end
	local definer = if kind == "packet" then "definePacket" else "defineQuery"
	local names = {}
	for name, definition in definitions do
		if type(name) ~= "string" then
			error(`[DogNet] namespace "{namespace}": {kind} names must be strings`, 4)
		end
		if type(definition) ~= "table" or rawget(definition, "kind") ~= kind then
			error(`[DogNet] namespace "{namespace}": "{name}" must be made with {definer}`, 4)
		end
		table.insert(names, name)
	end
	table.sort(names)
	return names
end

local function defineNamespace(name: string, build: any)
	if type(name) ~= "string" or name == "" then
		error("[DogNet] defineNamespace needs a name", 2)
	end
	local definitions = if type(build) == "function" then (build :: () -> any)() else build
	if type(definitions) ~= "table" then
		error("[DogNet] defineNamespace's function must return { packets = {...}, queries = {...} }", 2)
	end

	local packetDefinitions = definitions.packets or {}
	local queryDefinitions = definitions.queries or {}
	local packetNames = sortedNames(packetDefinitions, "packet", name)
	local queryNames = sortedNames(queryDefinitions, "query", name)

	-- Both sides hash the same schema description; a mismatch is reported
	-- instead of silently misreading data.
	local schema = {}
	for _, packetName in packetNames do
		local definition = packetDefinitions[packetName]
		table.insert(
			schema,
			`P:{packetName}:{if definition.reliable then "r" else "u"}:{(definition.codec :: Codec).desc}`
		)
	end
	for _, queryName in queryNames do
		local definition = queryDefinitions[queryName]
		table.insert(schema, `Q:{queryName}:{(definition.request :: Codec).desc}>{(definition.response :: Codec).desc}`)
	end

	local base = Registry.reserve(name, #packetNames + 2 * #queryNames, Registry.fingerprint(table.concat(schema, ";")))

	local packets = {}
	for index, packetName in packetNames do
		local definition = packetDefinitions[packetName]
		packets[packetName] = Packet(`{name}.{packetName}`, base + index - 1, definition.codec, definition.reliable)
	end

	local queries = {}
	local queryBase = base + #packetNames
	for index, queryName in queryNames do
		local definition = queryDefinitions[queryName]
		queries[queryName] = Query.create(
			`{name}.{queryName}`,
			queryBase + (index - 1) * 2,
			definition.request,
			definition.response,
			definition.timeout
		)
	end

	Registry.announce()

	return {
		packets = packets,
		queries = queries,
	}
end

local DogNet = {
	defineNamespace = defineNamespace,
	definePacket = definePacket,
	defineQuery = defineQuery,

	bool = Serdes.bool,
	uint8 = Serdes.uint8,
	uint16 = Serdes.uint16,
	uint32 = Serdes.uint32,
	int8 = Serdes.int8,
	int16 = Serdes.int16,
	int32 = Serdes.int32,
	float16 = Serdes.float16,
	float32 = Serdes.float32,
	float64 = Serdes.float64,
	varuint = Serdes.varuint,
	varint = Serdes.varint,
	string = Serdes.string,
	buff = Serdes.buff,
	vec2 = Serdes.vec2,
	vec3 = Serdes.vec3,
	vec3f16 = Serdes.vec3f16,
	color3 = Serdes.color3,
	cframe = Serdes.cframe,
	cframeCompact = Serdes.cframeCompact,
	inst = Serdes.inst,
	unknown = Serdes.unknown,
	auto = Serdes.auto,
	nothing = Serdes.nothing,

	struct = Serdes.struct,
	array = Serdes.array,
	map = Serdes.map,
	optional = Serdes.optional,
	enum = Serdes.enum,
	enumItem = Serdes.enumItem,
}

return (table.freeze(DogNet) :: any) :: Types.DogNet
