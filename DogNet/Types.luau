--!strict

-- `player` is the sender on the server and nil on the client.
export type Packet<T> = {
	-- Server only
	sendTo: (data: T, player: Player) -> (),
	sendToAll: (data: T) -> (),
	sendToList: (data: T, players: { Player }) -> (),
	sendToAllExcept: (data: T, except: Player) -> (),
	-- Client only
	send: (data: T) -> (),
	-- Both sides; listen and listenOnce return a disconnect function
	listen: (callback: (data: T, player: Player) -> ()) -> () -> (),
	listenOnce: (callback: (data: T, player: Player) -> ()) -> () -> (),
	wait: () -> (T, Player),
	disconnectAll: () -> (),
}

-- Either side can invoke. The server passes the player to ask; the client
-- leaves it out. invoke yields for the response and errors on failure or timeout.
export type Query<Request, Response> = {
	invoke: (request: Request, player: Player?) -> Response,
	listen: (handler: (request: Request, player: Player) -> Response) -> () -> (),
	disconnectAll: () -> (),
}

export type PacketProps<T> = {
	value: T,
	reliabilityType: ("reliable" | "unreliable")?,
}

export type QueryProps<Request, Response> = {
	request: Request,
	response: Response,
	-- Seconds before invoke gives up; defaults to the QUERY_TIMEOUT attribute (15). math.huge waits forever.
	timeout: number?,
}

-- Data types are typed as the values they carry, so a packet's schema also
-- types its data: struct({ hp = uint8 }) is { hp: number }.
export type DogNet = {
	defineNamespace: <T>(name: string, definitions: () -> T) -> T,
	definePacket: <T>(props: PacketProps<T>) -> Packet<T>,
	defineQuery: <Request, Response>(props: QueryProps<Request, Response>) -> Query<Request, Response>,

	bool: boolean,
	uint8: number,
	uint16: number,
	uint32: number,
	int8: number,
	int16: number,
	int32: number,
	float16: number,
	float32: number,
	float64: number,
	varuint: number,
	varint: number,
	string: string,
	buff: buffer,
	vec2: Vector2,
	vec3: Vector3,
	vec3f16: Vector3,
	color3: Color3,
	cframe: CFrame,
	cframeCompact: CFrame,
	inst: Instance,
	unknown: unknown,
	auto: any,
	nothing: nil,

	struct: <T>(fields: T) -> T,
	array: <T>(item: T) -> { T },
	map: <K, V>(key: K, value: V) -> { [K]: V },
	optional: <T>(item: T) -> T?,
	enum: <T>(values: { T }) -> T,
	enumItem: (enum: Enum) -> EnumItem,
}

return nil
