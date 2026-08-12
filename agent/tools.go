package agent

import (
	"context"
	"strings"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

type ToolFuncResponse struct {
	Message string
	Result  any
}

type ToolChunk struct {
	ID              string
	ToolName        string
	ToolParams      string
	ApproveToolChan chan bool
}

type ToolFailedChunk struct {
	ID         string
	ToolName   string
	ToolParams string
	Reason     string
}

type ToolFunc func(...any) (ToolFuncResponse, error)

type Tools map[string]ToolFunc

// ToolManager is the function that... well, manages tool
// calls. It recieves the tool chunk (for the tool information),
// the tool function (to be able to run the tool), the tool call
// (for the MCP tools), and the chunk channel to be able to
// communicate with the main go routine.
func ToolManager(client *bifrost.Bifrost, tool ToolChunk, toolFunc ToolFunc, toolCall schemas.ChatAssistantMessageToolCall, chunkChan chan any) {

	// Here we use the ApproveToolChan located inside the ToolChunk
	// to wait for the the user to confirm (or reject) the tool.

	select {
	case approved := <-tool.ApproveToolChan:
		if approved {

			// Here we check whether a tool is an MCP tool or not.
			// This will tell us whether to execute it as an actual
			// function or through the Bifrost client.
			// The way we check is whether the tool name contains a "-".
			// This is kinda bad and unweildy, as we have to ban users
			// from creating any tool names with the character "-",
			// but there is (currently) no other way that I have found.

			if strings.Contains(tool.ToolName, "-") {
				response, bifrostErr := client.ExecuteChatMCPTool(schemas.NewBifrostContext(context.Background(), schemas.NoDeadline), &toolCall)

				if bifrostErr != nil {
					chunkChan <- ToolFailedChunk{
						ID:         tool.ID,
						ToolName:   tool.ToolName,
						ToolParams: tool.ToolParams,
						Reason:     bifrostErr.String(),
					}

					return
				}

				chunkChan <- response
			} else {

				var response ToolFuncResponse

				// The tool function, gotten from the tool
				// map, can be nil. Here we check for that.

				if toolFunc != nil {

					// If the function isn't nil, congrats and
					// we run it.

					response, err := toolFunc(tool.ToolParams)

					if err != nil {
						chunkChan <- ToolFailedChunk{
							ID:         tool.ID,
							ToolName:   tool.ToolName,
							ToolParams: tool.ToolParams,
							Reason:     err.Error(),
						}

						return
					}

					chunkChan <- response
				} else {

					// Otherwise, we send an error chunk notifying
					// the parent about the failure.

					chunkChan <- ToolFailedChunk{
						ID:         tool.ID,
						ToolName:   tool.ToolName,
						ToolParams: tool.ToolParams,
						Reason:     "tool function mismatch: tool function does not exist.",
					}
				}

				chunkChan <- response
			}

			return
		} else {

			// If the user rejected the tool, then we simply send
			// a ToolFailedChunk with Reason of "tool rejected".

			chunkChan <- ToolFailedChunk{
				ID:         tool.ID,
				ToolName:   tool.ToolName,
				ToolParams: tool.ToolParams,
				Reason:     "tool rejected",
			}

			return
		}
	}
}
