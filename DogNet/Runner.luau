--!strict

--[[
	Runs callbacks on a recycled thread instead of creating one per call (the
	approach from stravant's GoodSignal). A callback that yields keeps the
	thread it is on, and a fresh one is made for the next call.
]]

local freeThread: thread? = nil

local function runAndRelease(callback: (...any) -> (), ...: any)
	local acquired = freeThread
	freeThread = nil
	callback(...)
	freeThread = acquired
end

local function loop()
	while true do
		runAndRelease(coroutine.yield())
	end
end

return function(callback: (...any) -> (), ...: any)
	if freeThread == nil then
		local thread = coroutine.create(loop)
		coroutine.resume(thread)
		freeThread = thread
	end
	task.spawn(freeThread :: thread, callback, ...)
end
