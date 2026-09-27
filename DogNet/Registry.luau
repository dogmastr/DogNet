--!strict

--[[
	Hands out message ids. Each namespace gets a block of consecutive ids; inside
	it, packets and queries are ordered by name, so the only thing the client
	needs from the server is where the block starts. The server publishes that
	(plus a fingerprint of the schema) as attributes on one Configuration per
	namespace.
]]

local RunService = game:GetService("RunService")

local Serdes = require(script.Parent.Serdes)

local IS_SERVER = RunService:IsServer()
-- Id 0 is never used; id 1 carries query failures.
local FIRST_ID = 2
-- Ids of 128 and up take two bytes on the wire, which caps them here.
local MAX_ID = 32767

local Registry = {}

Registry.QUERY_ERROR_ID = 1
-- Per message id: what reads its payload, and the listeners its value goes to.
Registry.readers = {} :: { [number]: Serdes.Reader }
Registry.targets = {} :: { [number]: { Serdes.Listener } }

local nextId = FIRST_ID
local folder: Instance = nil :: any
local defined: { [string]: boolean } = {}
local definedCallbacks: { () -> () } = {}

function Registry.start(root: Instance)
	if IS_SERVER then
		local namespaces = Instance.new("Folder")
		namespaces.Name = "Namespaces"
		namespaces.Parent = root
		folder = namespaces
	else
		folder = root:WaitForChild("Namespaces")
	end
end

function Registry.fingerprint(text: string): number
	local hash = 5381
	for index = 1, #text do
		hash = (hash * 33 + string.byte(text, index)) % 4294967296
	end
	return hash
end

-- Reserves `count` consecutive ids for a namespace and returns the first.
function Registry.reserve(name: string, count: number, fingerprint: number): number
	if defined[name] then
		error(`[DogNet] namespace "{name}" is defined twice`, 3)
	end
	defined[name] = true

	if IS_SERVER then
		local base = nextId
		nextId += count
		if nextId - 1 > MAX_ID then
			error(`[DogNet] too many packets and queries: ids are limited to {MAX_ID}`, 3)
		end

		local record = Instance.new("Configuration")
		record.Name = name
		record:SetAttribute("Base", base)
		record:SetAttribute("Count", count)
		record:SetAttribute("Fingerprint", fingerprint)
		record.Parent = folder
		return base
	end

	local record = folder:WaitForChild(name, 5)
	if record == nil then
		warn(
			`[DogNet] still waiting for the server to define namespace "{name}"; make sure the server requires the same module`
		)
		record = folder:WaitForChild(name, math.huge)
	end
	assert(record, "unreachable")

	if record:GetAttribute("Count") ~= count or record:GetAttribute("Fingerprint") ~= fingerprint then
		error(
			`[DogNet] namespace "{name}" is defined differently on the server and the client. Both sides must require the same ModuleScript, and definitions must not depend on which side runs them.`,
			3
		)
	end
	return record:GetAttribute("Base") :: number
end

-- Lets the client transport retry data it held back for a namespace that wasn't defined yet.
function Registry.onDefined(callback: () -> ())
	table.insert(definedCallbacks, callback)
end

function Registry.announce()
	for _, callback in definedCallbacks do
		callback()
	end
end

-- Names the namespace an id belongs to, for messages.
function Registry.describe(id: number): string
	for _, record in folder:GetChildren() do
		local base = record:GetAttribute("Base")
		local count = record:GetAttribute("Count")
		if type(base) == "number" and type(count) == "number" and id >= base and id < base + count then
			return `namespace "{record.Name}"`
		end
	end
	return `message id {id}`
end

return Registry
