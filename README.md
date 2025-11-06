How to use
-> Create a folder .vscode
-> Create a file mcp.json
-> Add the server
####
{
	"servers": {
		"deltamcp": {
			"type": "stdio",
			"command": "/Users/jennifer/.local/bin/uv",
			"args": ["run", "--directory", "/Users/jennifer/Documents/source/deltamcp", "python", "src/server.py"]
		}
	},
	"inputs": []
}
####

Adjust accordingly for Windows path.
Start the server. Make sure it is running.
Configure githubco pilot chat to use the mcp. You can add context to chose the mcp server and then ask questions.

For example -> List all tables
-> Show all tables that contain the column custoemr_id