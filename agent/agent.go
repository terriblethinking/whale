package agent

import (
	"context"
	"slices"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

// The main Agent struct.
type Agent struct {
	Client        bifrost.Bifrost
	Provider      schemas.ModelProvider
	Model         string
	Messages      []schemas.ChatMessage
	Tools         Tools
	ToolSchemas   []schemas.ChatTool
	ToolWhitelist []string
	ToolBlacklist []string
}

// The text chunk the streaming agent outputs.
type AsyncAgentTextChunk struct {
	Content string
}

// The reasoning chunk the streaming agent outputs.
type AsyncAgentReasongingChunk struct {
	Index   int
	Type    string
	Content string
}

// The error chunk a streaming agent can output.
type AsyncAgentErrorChunk struct {
	Content string
}

// The Run function simply runs a one off interaction,
// prompt -> reponse. No tools, and no agentic capabilities.
// We might need to expand upon this sometime, but I feel that
// simply creating a YOLO mode for the async agent will be sufficient.
func (a *Agent) Run(ctx schemas.BifrostContext, message string) (*schemas.BifrostChatResponse, *schemas.BifrostError) {
	response, err := a.Client.ChatCompletionRequest(&ctx, &schemas.BifrostChatRequest{
		Provider: a.Provider,
		Model:    a.Model,
		Input: append(a.Messages,
			schemas.ChatMessage{
				Role: schemas.ChatMessageRoleUser,
				Content: &schemas.ChatMessageContent{
					ContentStr: schemas.Ptr(message),
				},
			}),
	})

	if err != nil {
		return response, err
	}

	return response, nil
}

// The RunAsync function is the engine of the agent loop.
// Mostly everything happens in here.
func (a *Agent) RunAsync(message string) <-chan any {
	chunkChan := make(chan any, 100)

	// Run all agent stuff in a goroutine
	// to make it async.
	go func() {
		// Start a stream with Bifrost, using the agent settings and history.
		stream, err := a.Client.ChatCompletionStreamRequest(schemas.NewBifrostContext(context.Background(), schemas.NoDeadline), &schemas.BifrostChatRequest{
			Provider: a.Provider,
			Model:    a.Model,
			Input: append(a.Messages,
				schemas.ChatMessage{
					Role: schemas.ChatMessageRoleUser,
					Content: &schemas.ChatMessageContent{
						ContentStr: schemas.Ptr(message),
					},
				}),
			Params: &schemas.ChatParameters{
				Tools: a.ToolSchemas,
			},
		})

		if err != nil {
			chunkChan <- AsyncAgentErrorChunk{
				Content: err.String(),
			}

			return
		}

		for chunk := range stream {
			if chunk.BifrostChatResponse != nil && len(chunk.BifrostChatResponse.Choices) > 0 {
				choice := chunk.BifrostChatResponse.Choices[0]

				// Check whether there is any actual streaming content.
				if choice.ChatStreamResponseChoice != nil &&
					choice.ChatStreamResponseChoice.Delta != nil {

					// If the stream content is basic reasoning or response
					// text, return it to the client right away.
					if choice.ChatStreamResponseChoice.Delta.Content != nil && *choice.ChatStreamResponseChoice.Delta.Content != "" {
						content := *choice.ChatStreamResponseChoice.Delta.Content
						chunkChan <- AsyncAgentTextChunk{
							Content: content,
						}
					}

					if choice.ChatStreamResponseChoice.Delta.Reasoning != nil && *choice.ChatStreamResponseChoice.Delta.Reasoning != "" {
						content := *choice.ChatStreamResponseChoice.Delta.Reasoning
						chunkChan <- AsyncAgentReasongingChunk{
							Content: content,
						}
					}

					// If the streaming content involves tools, then we have
					// a slightly different procedure.
					if len(choice.ChatStreamResponseChoice.Delta.ToolCalls) > 0 {
						for _, tool := range choice.ChatStreamResponseChoice.Delta.ToolCalls {

							// If the tool is in the whitelist we skip
							// checking whether it is in the blacklist.
							// The whitelist overrules the blacklist.
							if slices.Contains(a.ToolWhitelist, *tool.Function.Name) {

								// Create an ApproveToolChannel for each tool call,
								// which will be used to approve that specific tool.
								ApproveToolChannel := make(chan bool)

								toolChunk := ToolChunk{
									ID:              *tool.ID,
									ToolName:        *tool.Function.Name,
									ToolParams:      tool.Function.Arguments,
									ApproveToolChan: ApproveToolChannel,
								}

								// Here we launch the ToolManager function. It takes
								// in the toolChunk, the tool function, and the chunkChan.
								// Using the ApproveToolChan we provide through the
								// toolChunk, it will wait for a response from the
								// main go routine (essentially the client or user)
								// with a reject or approval of the tool. Following
								// that, it will then run the tool and return the
								// result through chunkChan which we provided.
								go ToolManager(&a.Client, toolChunk, a.Tools[*tool.Function.Name], tool, chunkChan)

								// After passing the tool into ToolManager, we can send
								// it off to the client through chunkChan
								chunkChan <- toolChunk

							} else if slices.Contains(a.ToolBlacklist, *tool.Function.Name) || slices.Contains(a.ToolBlacklist, "*") {

								// If the tool is in the blacklist (or the
								// user added "*" to the blacklist, which
								// automatically adds all tools), and
								// not in the whitelist, then we send a
								// AsyncAgentToolFailedChunk error to the client
								// to tell them that the tool is blacklisted.
								toolBlacklistedErrorChunk := ToolFailedChunk{
									ID:         *tool.ID,
									ToolName:   *tool.Function.Name,
									ToolParams: tool.Function.Arguments,
									Reason:     "tool in blacklist.",
								}

								chunkChan <- toolBlacklistedErrorChunk
							} else {

								// If the tool isn't found in either the blacklist
								// or the whitelist, we simply send to back to the user.
								ApproveToolChannel := make(chan bool)

								toolChunk := ToolChunk{
									ID:              *tool.ID,
									ToolName:        *tool.Function.Name,
									ToolParams:      tool.Function.Arguments,
									ApproveToolChan: ApproveToolChannel,
								}

								// As above, we launch the ToolManager function. It takes
								// in the toolChunk, the tool function, and the chunkChan.
								// Using the ApproveToolChan we provide through the
								// toolChunk, it will wait for a response from the
								// main go routine (essentially the client or user)
								// with a reject or approval of the tool. Following
								// that, it will then run the tool and return the
								// result through chunkChan which we provided.
								go ToolManager(&a.Client, toolChunk, a.Tools[*tool.Function.Name], tool, chunkChan)

								// After passing the tool into ToolManager, we can send
								// it off to the client through chunkChan
								chunkChan <- toolChunk
							}
						}
					}
				}
			}
		}

		close(chunkChan)
	}()

	// Finally, we return chunkChan to the parent where
	// it can be processed.
	return chunkChan
}
