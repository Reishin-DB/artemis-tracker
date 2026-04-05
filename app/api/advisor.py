"""
/api/v1/advisor — Mission Advisor powered by Databricks Genie.
"""

import json
import logging
import os
import time
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["advisor"])

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "<your-genie-space-id>")


class ChatRequest(BaseModel):
    message: str
    conversation_id: str | None = None


def _query_genie(question: str, conversation_id: str | None = None) -> dict[str, Any]:
    """Send a question to Genie and return the response."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()

        if conversation_id:
            msg = w.genie.create_message(
                space_id=GENIE_SPACE_ID,
                conversation_id=conversation_id,
                content=question,
            )
            conv_id = conversation_id
            msg_id = msg.message_id
        else:
            conv = w.genie.start_conversation(
                space_id=GENIE_SPACE_ID,
                content=question,
            )
            conv_id = conv.conversation_id
            msg_id = conv.message_id

        # Poll for completion
        result = None
        for _ in range(20):
            time.sleep(2)
            result = w.genie.get_message(
                space_id=GENIE_SPACE_ID,
                conversation_id=conv_id,
                message_id=msg_id,
            )
            status = str(result.status)
            if "COMPLETED" in status or "FAILED" in status:
                break

        # Extract response
        text_parts = []
        sql_query = None

        if result and result.attachments:
            for att in result.attachments:
                if hasattr(att, 'text') and att.text and hasattr(att.text, 'content') and att.text.content:
                    text_parts.append(att.text.content)
                if hasattr(att, 'query') and att.query:
                    if hasattr(att.query, 'query') and att.query.query:
                        sql_query = att.query.query
                    if hasattr(att.query, 'description') and att.query.description:
                        text_parts.insert(0, att.query.description)

        response_text = "\n\n".join(text_parts) if text_parts else "I couldn't find an answer. Try rephrasing your question."

        return {
            "content": response_text,
            "sql": sql_query,
            "conversation_id": conv_id,
        }

    except Exception as e:
        logger.error("Genie query failed: %s", e)
        return {
            "content": f"Genie error: {str(e)[:150]}",
            "sql": None,
            "conversation_id": None,
        }


@router.post("/advisor")
def chat_with_advisor(req: ChatRequest):
    """Query Genie and return the response as JSON (not streaming)."""
    result = _query_genie(req.message, req.conversation_id)

    return {
        "content": result["content"],
        "conversation_id": result.get("conversation_id"),
    }
