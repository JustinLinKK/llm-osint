from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import requests

from env import load_env


class OpenRouterLLM:
    def __init__(self, model: str | None = None, api_key: str | None = None) -> None:
        load_env()
        api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY not set")

        self._model = model or os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
        self._api_key = api_key
        self._base_url = "https://openrouter.ai/api/v1"

    def plan_tools(
        self,
        prompt: str,
        inputs: List[str],
        tool_catalog: List[Dict[str, Any]],
        prior_tool_calls: List[Dict[str, Any]] | None = None,
        system_prompt: str | None = None,
    ) -> Dict[str, Any]:
        system = system_prompt or (
            "You are a planning agent. Return JSON only. "
            "Decide which tools to call and if more info is needed."
        )
        user = {
            "prompt": prompt,
            "inputs": inputs,
            "tool_catalog": tool_catalog,
            "prior_tool_calls": prior_tool_calls or [],
            "output_schema": {
                "plan": [{"tool": "string", "args": "object", "rationale": "string"}],
                "reasoning": "string",
                "rationale": "string",
                "urls": ["string"],
                "enough_info": "boolean"
            },
        }

        data = self.complete_json(system, user, temperature=0.2, timeout=30)

        plan = data.get("plan") if isinstance(data.get("plan"), list) else []
        reasoning = data.get("reasoning")
        rationale = data.get("rationale")

        return {
            "plan": plan,
            "rationale": str(reasoning or rationale or ""),
            "urls": data.get("urls", []) if isinstance(data.get("urls"), list) else [],
            "enough_info": bool(data.get("enough_info", False)),
        }

    def refine_tool_arguments(
        self,
        system_prompt: str,
        tool_name: str,
        arguments: Dict[str, Any],
    ) -> Dict[str, Any]:
        user = {
            "tool": tool_name,
            "arguments": arguments,
            "output_schema": {"arguments": "object"},
            "instructions": "Return JSON only. Keep runId unchanged.",
        }

        parsed = self.complete_json(system_prompt, user, temperature=0.1, timeout=30)

        refined = parsed.get("arguments") if isinstance(parsed, dict) else None
        if isinstance(refined, dict):
            return refined
        return arguments

    def complete_json(
        self,
        system_prompt: str,
        user_payload: Dict[str, Any],
        temperature: float = 0.1,
        timeout: int = 30,
    ) -> Dict[str, Any]:
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload)},
            ],
            "temperature": temperature,
        }

        response = requests.post(
            f"{self._base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            data=json.dumps(payload),
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "{}")
        )
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def generate_run_title(self, prompt: str, inputs: List[str]) -> str | None:
        system = (
            "You generate concise investigation titles. "
            "Return JSON only with schema: {\"title\":\"string\"}. "
            "Rules: 4-10 words, specific, no quotes, no trailing period."
        )
        user = {
            "prompt": prompt,
            "inputs": inputs[:8],
        }

        parsed = self.complete_json(system, user, temperature=0.2, timeout=30)

        title = parsed.get("title") if isinstance(parsed, dict) else None
        if not isinstance(title, str):
            return None
        normalized = " ".join(title.strip().split())
        if not normalized:
            return None
        return normalized[:160]
