/*
	RedisInc.inc - Include implementing a Redis client / Parser
	Copyright (C) 2020 Kinsi

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#if defined _redisinc_included
 #endinput
#endif
#define _redisinc_included

#include <socket>
#include <NestedStringMap>

#define REDISINC_MAXARRAYVALS 8192
#define REDISINC_MAXSTRINGSIZE 2048

//#define DEBUG_REDISINC

#if defined DEBUG_REDISINC
	#pragma dynamic 32768
	
	void PrintDebugString(int indent, char[] format, any ...) {
		char[] indenter = new char[indent];
		
		for(int i = 0; i < indent; i++)
			indenter[i] = '\t';
			
		char target[1024];
		VFormat(target, sizeof(target), format, 3);
		ReplaceString(target, sizeof(target), "\n", "\\n");
		ReplaceString(target, sizeof(target), "\r", "\\r");
	
		PrintToServer("[Redis] %s%s", indenter, target);
	}
#endif

stock bool SubString(const char[] source, int start, int len, char[] destination, int maxlen) {
	if(maxlen < 1)
		ThrowError("Destination size must be 1 or greater, but was %d", maxlen);
	
	//optimization
	if(len == 0) {
		destination[0] = '\0';
		return true;
	}
	
	if(start < 0) {
		//strlen doesn't count the null terminator, so don't -1 on it.
		start = strlen(source) + start;
		if(start < 0)
			start = 0;
	}
	
	if(len < 0) {
		len = strlen(source) + len - start;
		//If length is still less than 0, that'd be an error.
		if(len < 0)
			return false;
	}
	
	//Check to make sure destination is large enough to hold the len, or truncate it.
	//len + 1 because second arg to strcopy counts 1 for the null terminator
	int realLength = len + 1 < maxlen ? len + 1 : maxlen;
	
	strcopy(destination, realLength, source[start]);
	return true;
}


enum REDIS_RESPTYPE {
	RESPTYPE_STRING,
	RESPTYPE_INTEGER,
	RESPTYPE_BULKSTRING,
	RESPTYPE_ARRAY,
	RESPTYPE_NULL,
	RESPTYPE_ERROR
}

enum REDIS_PARSETYPE {
	REDISRESPONSE_IGNORE,
	REDISRESPONSE_PARSE_FULL,
	//REDISRESPONSE_PARSE_STRINGCELLS //Unimplemented atm, is supposed to, instead of deep parsing, just parse everything as a string. Unsure if this even possible
}

#define REDISINC_RESPINDEX_TYPE "RT~_#§°"
#define REDISINC_RESPINDEX_DATA "RD~_#§°"
#define REDISINC_RESPINDEX_DATALEN "RL~_#§°"

methodmap RedisResponse < NestedStringMap {
	public RedisResponse(REDIS_RESPTYPE responseType = RESPTYPE_NULL) {
		NestedStringMap newNSM = new NestedStringMap();
		
		newNSM.SetValue(REDISINC_RESPINDEX_TYPE, responseType);
		
		return view_as<RedisResponse>(newNSM);
	}
	
	property REDIS_RESPTYPE Type {
		public get() {
			return view_as<REDIS_RESPTYPE>(this.GetAndReturnValue(REDISINC_RESPINDEX_TYPE));
		}
	}
	
	property int Length {
		public get() {
			if(this.Type == RESPTYPE_NULL)
				return 0;
			
			return this.GetAndReturnValue(REDISINC_RESPINDEX_DATALEN, 0);
		}
	}
	
	/*public void GetRawResponseString(char[] buffer, int maxlength) {
		this.GetString(REDISINC_RESPINDEX_RAW, buffer, maxlength);
	}*/
	
	public bool GetString(char[] buffer, int maxlength) {
		if(this.Type == RESPTYPE_STRING || this.Type == RESPTYPE_BULKSTRING || this.Type == RESPTYPE_ERROR) {
			this.GetString(REDISINC_RESPINDEX_DATA, buffer, maxlength);
			return true;
		} else return false;
	}
	
	public int GetInt() {
		if(this.Type == RESPTYPE_INTEGER)
			return this.GetAndReturnValue(REDISINC_RESPINDEX_DATA);
		else return -1;
	}
	
	public RedisResponse Subresponse(int index) {
		static char subKey[16];
		
		if(index >= this.Length || index < 0)
			ThrowError("Out of bounds (Requested: %i, Length: %i)", index + 1, this.Length);
		
		IntToString(index, subKey, 16);
		
		return view_as<RedisResponse>(this.Child(subKey));
	}
}

methodmap FifoList < ArrayList {
	public FifoList(int blocksize = 1) {
		return view_as<FifoList>(new ArrayList(blocksize));
	}

	public void PopArray(any[] buffer) {
		GetArrayArray(this, 0, buffer);
		RemoveFromArray(this, 0);
	}
	
	property bool Empty {
		public get() { return !this.Length; }
	}
}

typedef RedisResponseCb = function void(RedisResponse response, int extraInfo);
static RedisResponseCb NullResponse;

enum struct RedisCallback {
	Function ResponseCb;
	int extraInfo;
	REDIS_PARSETYPE parseType;
}


enum struct RedisConn {
	Handle socket;
	
	FifoList cbQueue;
	
	bool isConnected() {
		return this.socket && SocketIsConnected(this.socket);
	}
	
	
	void Connect(const char[] host = "127.0.0.1", int port = 6379, int maxCallbacksPerFrame = 1) {
		if(this.isConnected())
			ThrowError("We're already connected!");
	
		if(this.cbQueue == null)
			this.cbQueue = new FifoList(sizeof(RedisCallback));
		
		this.socket = SocketCreate(SOCKET_TCP, RedisIncOnSocketError);
		
		SocketSetOption(this.socket, SocketReuseAddr, true);
		SocketSetOption(this.socket, SocketKeepAlive, true);
		SocketSetOption(this.socket, CallbacksPerFrame, maxCallbacksPerFrame);
		
		//WORKAROUND We need to hardcore the size for the array because sizeof(RedisConn) returns 0 here. SM quirks
		ArrayList foo = new ArrayList(2);
		foo.PushArray(this);
		
		SocketSetArg(this.socket, foo);
		
		SocketConnect(this.socket, RedisIncOnSocketConnected, RedisIncOnSocketReceive, RedisIncOnSocketDisconnected, host, port);
	}
	
	bool Execute(const char[] command, RedisResponseCb callback, int extraInfo = 0, REDIS_PARSETYPE parseType = REDISRESPONSE_PARSE_FULL, bool hasTrailingNewline = false) {
		if(!this.isConnected()) 
			return false;
			
		#if defined DEBUG_REDISINC
			PrintDebugString(0, "Executing command '%s', Parse response: %i", command, parseType);
		#endif
		
		RedisCallback newCb;
		newCb.ResponseCb = callback;
		newCb.extraInfo = extraInfo;
		newCb.parseType = parseType;
		
		this.cbQueue.PushArray(newCb);
			
		if(hasTrailingNewline) {
			SocketSend(this.socket, command);
		} else {
			char theCmd[REDISINC_MAXSTRINGSIZE];
			
			Format(theCmd, sizeof(theCmd), "%s\r\n", command);
			
			SocketSend(this.socket, theCmd);
		}

		return true;
	}
	
	bool ExecuteAndForget(char[] command, bool hasTrailingNewline = false) {
		return this.Execute(command, NullResponse, _, REDISRESPONSE_IGNORE, hasTrailingNewline);
	}
	
	void Disconnect() {
		if(this.isConnected())
			this.ExecuteAndForget("QUIT");
	}
}


/*
	Event handlers
*/

#define _con_from_arg RedisConn con; arg.GetArray(0, con)

static void RedisIncOnSocketConnected(Handle socket, ArrayList arg) {
	PrintToServer("[Redis] Connected to server");
	
	//CurrentConnection.SendAndForget("PING");
}

static void RedisIncOnSocketDisconnected(Handle socket, ArrayList arg) {
	CloseHandle(socket);
	
	PrintToServer("[Redis] CONNECTION DISCONNECT!!");
}

static void RedisIncOnSocketError(Handle socket, const int errorType, const int errorNum, ArrayList arg) {
	RedisIncOnSocketDisconnected(socket, arg);
	
	//PrintToServer("[Redis] CONNECTION ERROR!! %i %i", errorType, errorNum);
}

/*
	Parser
*/


#define CR '\r' //13

#define REDISPROT_STRING '+'
#define REDISPROT_INTEGER ':'
#define REDISPROT_BULKSTRING '$'
#define REDISPROT_ARRAY '*'
#define REDISPROT_ERROR '-'


enum struct RedisParser {
	int iReadOffset;
	char cMessageData[16384];
	int iMessageDataLength;
	
#if defined DEBUG_REDISINC
	int currentDepth;
#endif
	
	int ParseNumber(int &length = 0) {
		//int left = this.iMessageDataLength - 1;
		
		int num = 0;
		int sign = 1;
		
		length = 0;
		
		if(this.cMessageData[this.iReadOffset] == '-') {
			sign = -1;
			this.iReadOffset++;
		}
		
		while(this.iReadOffset < this.iMessageDataLength) {
			char curChar = this.cMessageData[this.iReadOffset];
			
			if(curChar == CR) {
				this.iReadOffset += 2; //Skip the following LF also
				
				return num * sign;
			}
			
			this.iReadOffset++;
			
			length++;
			
			num = (num * 10) + (curChar - 48);
		}
		
		return -1;
	}
	
	int ParseSimpleNumber(RedisResponse resp) {
		int length = 0;
		int num = this.ParseNumber(length);
		
		resp.SetValue(REDISINC_RESPINDEX_DATALEN, length);
		resp.SetValue(REDISINC_RESPINDEX_DATA, num);
		
		#if defined DEBUG_REDISINC
				PrintDebugString(this.currentDepth, "Parsed Number of length %i: %i", length, num);
		#endif
		
		return num;
	}
	
	int ParseSimpleString(RedisResponse resp) {
		int start = this.iReadOffset;
		
		while(this.iReadOffset < this.iMessageDataLength) {
			if(this.cMessageData[this.iReadOffset] == CR) {
				this.iReadOffset += 2; //Skip \r\n
				break;
			}
			
			this.iReadOffset++;
		}
		
		int length = this.iReadOffset - start;
		
		char[] readString = new char[length + 1];
		strcopy(readString, length + 1, this.cMessageData[start]);
		
		
		#if defined DEBUG_REDISINC
			PrintDebugString(this.currentDepth, "Parsed String of length %i: %s", length, readString);
		#endif
		
		resp.SetString(REDISINC_RESPINDEX_DATA, readString);
		resp.SetValue(REDISINC_RESPINDEX_DATALEN, length);
	
		return length;
	}
	
	int ParseBulkString(RedisResponse resp) {
		int length = this.ParseNumber();
		
		if(length < 0)
			resp.SetValue(REDISINC_RESPINDEX_TYPE, RESPTYPE_NULL);
			
		int offset = 0;
		
		char readString[REDISINC_MAXSTRINGSIZE];
		if(length > 0) {
			offset = this.iReadOffset + length;
			
			if(offset + 2 > this.iMessageDataLength) {
				//SubString(this.cMessageData, this.iReadOffset, length, readString, REDISINC_MAXSTRINGSIZE);
			} else {
				SubString(this.cMessageData, this.iReadOffset, length, readString, REDISINC_MAXSTRINGSIZE);
				
				this.iReadOffset = offset + 2;
			}
		}
		
		resp.SetString(REDISINC_RESPINDEX_DATA, readString);
		resp.SetValue(REDISINC_RESPINDEX_DATALEN, length >= 0 ? length : 0);
		
		#if defined DEBUG_REDISINC
			PrintDebugString(this.currentDepth, "Parsed Bulk String of length %i: %s", length, readString);
		#endif
	
		return offset;
	}
	
	void ParseArray(RedisResponse resp) {
		int length = this.ParseNumber();
		
		resp.SetValue(REDISINC_RESPINDEX_DATALEN, length);
		
		#if defined DEBUG_REDISINC
				PrintDebugString(this.currentDepth, "Attempting to parse Array response of size %i", length);
		#endif
		
		if(length <= 0)
			return;
			
		#if defined DEBUG_REDISINC
			this.currentDepth += 1;
		#endif
			
		this.ParseArrayElements(resp);
		
		#if defined DEBUG_REDISINC
			this.currentDepth -= 1;
		#endif
	}
	
	void ParseArrayElements(RedisResponse resp) {
		#if defined DEBUG_REDISINC
			int elemNum = 0;
		#endif
	
		for(int pos = 0; pos < resp.Length; pos++) {
			if(this.iReadOffset > this.iMessageDataLength)
				return;
			
			#if !defined DEBUG_REDISINC
				this.ParseInto(resp.Subresponse(pos))
			#else
				PrintDebugString(this.currentDepth, "Parsing array element #%i...", elemNum++);
				
				this.currentDepth += 1;
				REDIS_RESPTYPE type = this.ParseInto(resp.Subresponse(pos));
				
				PrintDebugString(this.currentDepth, "Done parsing array element (Parsed as Type %i)", type);
				this.currentDepth -= 1;
			#endif
		}
	}
	
	REDIS_RESPTYPE ParseInto(RedisResponse resp) {
		#define setRespType(%1) resp.SetValue(REDISINC_RESPINDEX_TYPE, %1)
	
		switch(this.cMessageData[this.iReadOffset++]) {
			case REDISPROT_STRING: {
				setRespType(RESPTYPE_STRING);
				
				this.ParseSimpleString(resp);
				
				return RESPTYPE_STRING;
			}
			case REDISPROT_INTEGER: {
				setRespType(RESPTYPE_INTEGER);
				
				this.ParseSimpleNumber(resp);
				
				return RESPTYPE_INTEGER;
			}
			case REDISPROT_BULKSTRING: {
				setRespType(RESPTYPE_BULKSTRING);
				
				this.ParseBulkString(resp);
				
				return RESPTYPE_BULKSTRING;
			}
			case REDISPROT_ARRAY: {
				setRespType(RESPTYPE_ARRAY);
				
				this.ParseArray(resp);
				
				return RESPTYPE_ARRAY;
			}
			case REDISPROT_ERROR: {
				setRespType(RESPTYPE_ERROR);
				
				this.ParseSimpleString(resp);
			}
			default: {
				ThrowError("Unknown message Type, cannot parse");
			}
		}
		
		return RESPTYPE_ERROR;
	}
}

static void RedisIncOnSocketReceive(Handle socket, char[] receiveData, const int dataSize, ArrayList arg) {
	_con_from_arg;
	
	int curpos = 0;
	
	while(curpos < dataSize) {
		//Parse Response
		RedisCallback respExtraData;
		con.cbQueue.PopArray(respExtraData);
		
		RedisResponse resp = new RedisResponse();
		
		RedisParser parser;
		
		strcopy(parser.cMessageData, sizeof(parser.cMessageData), receiveData[curpos]);
		parser.iMessageDataLength = dataSize;
		
		#if defined DEBUG_REDISINC
			parser.currentDepth = 0;
			PrintDebugString(0, "We will attempt to parse a response, starting from '%s'", receiveData[curpos])
			PrintToServer("=============\n");
		#endif
	
		//int startOffset = parser.iReadOffset;
		parser.ParseInto(resp);
		
		if(respExtraData.parseType != REDISRESPONSE_IGNORE) {
			Call_StartFunction(INVALID_HANDLE, respExtraData.ResponseCb);
			Call_PushCell(resp);
			Call_PushArray(respExtraData, sizeof(respExtraData));
			Call_Finish();
		}
		
		curpos += parser.iReadOffset;
	}
}