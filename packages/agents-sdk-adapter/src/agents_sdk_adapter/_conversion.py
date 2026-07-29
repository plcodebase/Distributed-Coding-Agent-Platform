"""Conversions between provider-neutral core contracts and Agents SDK types."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from agents import FunctionTool

from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import GatewayMessage, GatewayRequest, GatewayToolDefinition, MessageRole

if TYPE_CHECKING:
    from collections.abc import Sequence

    from agents.items import TResponseInputItem
    from agents.tool import Tool
    from agents.tool_context import ToolContext
    from openai.types.responses import (
        EasyInputMessageParam,
        ResponseFunctionToolCallParam,
        ResponseOutputMessageParam,
        ResponseOutputTextParam,
    )
    from openai.types.responses.response_input_item_param import FunctionCallOutput


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def convert_request_input(
    request: GatewayRequest,
) -> tuple[str | None, list[TResponseInputItem]]:
    """Convert a validated core request without changing conversation semantics."""

    system_messages: list[str] = []
    input_items: list[TResponseInputItem] = []
    conversation_started = False

    for message_index, message in enumerate(request.messages):
        if message.role is MessageRole.SYSTEM:
            if conversation_started:
                raise DomainOperationError(
                    code="invalid_gateway_request",
                    message="system messages must precede conversation messages",
                    details={
                        "model_call_id": request.model_call_id,
                        "request_id": request.request_id,
                    },
                )
            system_messages.append(message.content)
            continue

        conversation_started = True
        _append_message_items(
            input_items,
            message,
            message_index=message_index,
        )

    instructions = "\n\n".join(system_messages) or None
    return instructions, input_items


def _append_message_items(
    input_items: list[TResponseInputItem],
    message: GatewayMessage,
    *,
    message_index: int,
) -> None:
    if message.role is MessageRole.USER:
        user_message: EasyInputMessageParam = {
            "role": "user",
            "content": message.content,
            "type": "message",
        }
        input_items.append(user_message)
        return

    if message.role is MessageRole.ASSISTANT:
        if message.content:
            output_text: ResponseOutputTextParam = {
                "annotations": [],
                "text": message.content,
                "type": "output_text",
            }
            assistant_message: ResponseOutputMessageParam = {
                "id": f"message_{message_index}",
                "content": [output_text],
                "role": "assistant",
                "status": "completed",
                "type": "message",
            }
            input_items.append(assistant_message)
        for tool_call in message.tool_calls:
            function_call: ResponseFunctionToolCallParam = {
                "arguments": _canonical_json(tool_call.arguments.to_json_object()),
                "call_id": tool_call.id,
                "name": tool_call.name,
                "type": "function_call",
            }
            input_items.append(function_call)
        return

    if message.role is MessageRole.TOOL:
        if message.tool_call_id is None:
            raise AssertionError("validated tool messages always contain tool_call_id")
        tool_output: FunctionCallOutput = {
            "call_id": message.tool_call_id,
            "output": message.content,
            "type": "function_call_output",
        }
        input_items.append(tool_output)
        return

    raise AssertionError(f"unsupported validated message role: {message.role}")


async def _unreachable_tool_callback(
    context: ToolContext[Any],
    arguments_json: str,
) -> Any:
    del context, arguments_json
    raise RuntimeError("Agents SDK tool callbacks are disabled at the model boundary")


def convert_tool_definitions(
    definitions: Sequence[GatewayToolDefinition],
) -> list[Tool]:
    """Create schema-only SDK tools whose callbacks can never execute."""

    return [
        FunctionTool(
            name=definition.name,
            description=definition.description,
            params_json_schema=definition.input_schema.to_json_object(),
            on_invoke_tool=_unreachable_tool_callback,
            strict_json_schema=False,
        )
        for definition in definitions
    ]


__all__ = ["convert_request_input", "convert_tool_definitions"]
