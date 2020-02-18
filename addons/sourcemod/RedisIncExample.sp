
#include <sourcemod>
#include <RedisInc>

public Plugin myinfo = {
	name = "Redis Include",
	author = "Kinsi",
	description = "¯\\_(ツ)_//¯",
	version = "1337",
	url = "kinsi.me"
};

RedisConn x;

public void OnPluginStart() {
	x.Connect("192.168.178.47");

	CreateTimer(0.1, SendDataABitLater);
}


Action SendDataABitLater(Handle timer) {
	x.Execute("INVALID COMMAND", MyRedisResponse);
	x.Execute("GET whatever", MyRedisResponse);
	x.Execute("MEMORY STATS", MyRedisResponse);
	//x.Send("SSCAN settest 0 COUNT 10000", MyRedisResponse);
}

void MyRedisResponse(RedisResponse response, int extraInfo) {
	if(response.Type == RESPTYPE_STRING || response.Type == RESPTYPE_BULKSTRING) {
		char respStr[REDISINC_MAXSTRINGSIZE];
		response.GetString(respStr, sizeof(respStr));

		PrintToServer("Resp String: >>> '%s'", respStr);

		return;
	} else if(response.Type == RESPTYPE_ERROR) {
		char respStr[REDISINC_MAXSTRINGSIZE];
		response.GetString(respStr, sizeof(respStr));

		PrintToServer("Resp Error: >>> '%s'", respStr);

		return;
	} else if(response.Type == RESPTYPE_INTEGER) {
		PrintToServer("RESPONSE INT: >>> '%i'", response.GetInt());
	} else if(response.Type == RESPTYPE_ARRAY) {
		PrintToServer("RESPONSE Array of Length: >>> '%i'", response.Length);
	} else {
		//char respStr[REDISINC_MAXSTRINGSIZE];
		//response.GetRawResponseString(respStr, sizeof(respStr));

		PrintToServer("RAW RESPONSE STRING");
	}
}