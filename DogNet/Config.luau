--!strict

--[[
	Settings, read once from attributes on the DogNet ModuleScript. Add them
	in Studio's Properties panel (or set them before DogNet is first required).
	Attribute names match ByteNet Max where the setting exists there.
]]

local root = script.Parent

local function number(name: string, default: number): number
	local value = root:GetAttribute(name)
	return if type(value) == "number" then value else default
end

local maxBatchSize = number("MAX_BUFFER_SIZE", 8192)

return table.freeze({
	-- Largest batch the server accepts from one client, in bytes. Also the size
	-- of each client's byte budget.
	maxBatchSize = maxBatchSize,
	-- Bytes per second that budget refills.
	rateLimit = number("RATE_LIMIT", maxBatchSize),
	-- Unreliable batches are split to stay under this many bytes; Roblox drops
	-- larger ones (about 994 bytes measured, minus 6 per Instance).
	unreliableLimit = number("UNRELIABLE_LIMIT", 950),
	-- Seconds before invoke gives up, unless the query sets its own timeout.
	queryTimeout = number("QUERY_TIMEOUT", 15),
	-- Query requests one client may have running on the server at once.
	maxQueriesInFlight = number("MAX_QUERIES_IN_FLIGHT", 32),
	-- Rough memory, in bytes, each end of a player's stream may spend
	-- remembering dictionary key sets, per direction. The server's value is
	-- the one both sides use. 0 turns remembering off.
	keySetMemory = number("KEY_SET_MEMORY", 262144),
})
