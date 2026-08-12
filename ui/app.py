"""Streamlit control UI for the event processing API.

The UI deliberately keeps credentials in Streamlit's in-memory session state.
It does not contain a default API key or token; use the API key/token fields in
the sidebar when running it locally.
"""

from __future__ import annotations

import json
import os
from typing import Any, Literal

import requests
import streamlit as st


DEFAULT_API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
RequestFormat = Literal["json", "text"]


class ApiError(RuntimeError):
    """An API or transport error that can be shown to the operator."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ApiClient:
    """Small client for the existing FastAPI endpoints."""

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str = "",
        access_token: str = "",
        auth_mode: Literal["API key", "JWT token"] = "API key",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.access_token = access_token.strip()
        self.auth_mode = auth_mode

    def _headers(self, credential_mode: Literal["configured", "api_key", "none"]) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if credential_mode == "api_key" and self.api_key:
            headers["X-API-Key"] = self.api_key
        elif credential_mode == "configured":
            if self.auth_mode == "JWT token" and self.access_token:
                headers["Authorization"] = f"Bearer {self.access_token}"
            elif self.api_key:
                headers["X-API-Key"] = self.api_key
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: Any = None,
        extra_headers: dict[str, str] | None = None,
        response_format: RequestFormat = "json",
        credential_mode: Literal["configured", "api_key", "none"] = "configured",
    ) -> Any:
        """Call an endpoint and convert failures into explicit ``ApiError``s."""

        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = self._headers(credential_mode)
        if extra_headers:
            headers.update(extra_headers)
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=body,
                timeout=10,
            )
        except requests.RequestException as exc:
            raise ApiError(f"Could not reach {url}: {exc}") from exc

        if not 200 <= response.status_code < 300:
            detail: Any = response.text.strip()
            try:
                error_body = response.json()
            except ValueError:
                error_body = None
            if isinstance(error_body, dict) and error_body.get("detail") is not None:
                detail = error_body["detail"]
            elif error_body is not None:
                detail = error_body
            if not detail:
                detail = response.reason or "request failed"
            raise ApiError(f"{response.status_code} {response.reason}: {detail}", response.status_code)

        if response_format == "text":
            return response.text
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError(f"Expected JSON from {method} {path}, but received invalid JSON") from exc


def _show_error(operation: str, error: ApiError) -> None:
    st.error(f"{operation} failed: {error}")


def _parse_json_object(text: str, field_name: str) -> dict[str, Any] | None:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        st.error(f"{field_name} must be valid JSON: {exc.msg}")
        return None
    if not isinstance(value, dict):
        st.error(f"{field_name} must be a JSON object.")
        return None
    return value


def _parse_json_list(text: str, field_name: str) -> list[Any] | None:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        st.error(f"{field_name} must be valid JSON: {exc.msg}")
        return None
    if not isinstance(value, list):
        st.error(f"{field_name} must be a JSON list.")
        return None
    return value


def _render_sidebar() -> ApiClient:
    st.sidebar.header("Connection")
    base_url = st.sidebar.text_input("API base URL", value=DEFAULT_API_BASE_URL)
    api_key = st.sidebar.text_input("API key", type="password", help="Sent as X-API-Key.")

    if st.sidebar.button("Exchange API key for JWT"):
        if not api_key.strip():
            st.sidebar.error("Enter an API key before requesting a JWT.")
        else:
            token_client = ApiClient(base_url, api_key=api_key, auth_mode="API key")
            try:
                token_response = token_client.request(
                    "POST", "/auth/token", credential_mode="api_key"
                )
            except ApiError as error:
                _show_error("JWT request", error)
            else:
                st.session_state["jwt_token"] = token_response["access_token"]
                st.sidebar.success("JWT received; select JWT token below.")

    access_token = st.sidebar.text_input(
        "JWT access token",
        value=st.session_state.get("jwt_token", ""),
        type="password",
        key="jwt_token",
        help="Used as Authorization: Bearer <token>.",
    )
    auth_mode = st.sidebar.radio("Credential to use", ("API key", "JWT token"))
    st.sidebar.caption("Credentials are only held in this Streamlit session.")
    return ApiClient(
        base_url,
        api_key=api_key,
        access_token=access_token,
        auth_mode=auth_mode,
    )


def _render_overview(client: ApiClient) -> None:
    st.subheader("Service overview")
    health_col, metrics_col = st.columns(2)
    with health_col:
        if st.button("Check health"):
            try:
                st.json(client.request("GET", "/health", credential_mode="none"))
            except ApiError as error:
                _show_error("Health check", error)
    with metrics_col:
        if st.button("Load metrics"):
            try:
                st.code(client.request("GET", "/metrics", response_format="text", credential_mode="none"))
            except ApiError as error:
                _show_error("Metrics request", error)


def _render_publish(client: ApiClient) -> None:
    st.subheader("Publish event")
    with st.form("publish_event"):
        event_type = st.text_input("Event type", placeholder="order.created")
        source = st.text_input("Source", placeholder="checkout")
        payload_text = st.text_area("Payload (JSON object)", value='{"order_id": "o-1"}')
        idempotency_key = st.text_input("Idempotency key (optional)")
        submitted = st.form_submit_button("Publish")
    if submitted:
        payload = _parse_json_object(payload_text, "Payload")
        if payload is None or not event_type.strip() or not source.strip():
            if not event_type.strip():
                st.error("Event type is required.")
            if not source.strip():
                st.error("Source is required.")
            return
        extra_headers: dict[str, str] = {}
        if idempotency_key.strip():
            extra_headers["Idempotency-Key"] = idempotency_key.strip()
        try:
            result = client.request("POST", "/events", body={
                "event_type": event_type.strip(),
                "source": source.strip(),
                "payload": payload,
            }, extra_headers=extra_headers)
        except ApiError as error:
            _show_error("Publish event", error)
        else:
            st.success("Event accepted.")
            st.json(result)
            if extra_headers:
                st.info("Repeat with the same idempotency key to safely retry this request.")


def _render_pipelines(client: ApiClient) -> None:
    st.subheader("Pipelines")
    with st.form("create_pipeline"):
        event_type = st.text_input("Pipeline event type", placeholder="order.created")
        name = st.text_input("Pipeline name", placeholder="orders")
        steps_text = st.text_area("Steps (JSON list)", value='[{"type": "validate"}]')
        enabled = st.checkbox("Enabled", value=True)
        submitted = st.form_submit_button("Create or update pipeline")
    if submitted:
        steps = _parse_json_list(steps_text, "Steps")
        if steps is None:
            return
        if not event_type.strip() or not name.strip():
            st.error("Pipeline event type and name are required.")
            return
        try:
            result = client.request(
                "POST",
                "/pipelines",
                body={
                    "event_type": event_type.strip(),
                    "name": name.strip(),
                    "steps": steps,
                    "enabled": enabled,
                },
            )
        except ApiError as error:
            _show_error("Create pipeline", error)
        else:
            st.success("Pipeline saved.")
            st.json(result)

    if st.button("Load pipelines"):
        try:
            st.json(client.request("GET", "/pipelines"))
        except ApiError as error:
            _show_error("Load pipelines", error)


def _render_events(client: ApiClient) -> None:
    st.subheader("Events and history")
    limit = st.number_input("Maximum events", min_value=1, max_value=500, value=100)
    if st.button("Load events"):
        try:
            st.session_state["events"] = client.request(
                "GET", "/events", params={"limit": int(limit)}
            )
        except ApiError as error:
            _show_error("Load events", error)
    events = st.session_state.get("events", [])
    if events:
        st.dataframe(events, use_container_width=True)
        event_ids = [item["event_id"] for item in events if item.get("event_id")]
        selected_id = st.selectbox("Event history", event_ids)
        if st.button("Load selected history"):
            try:
                st.json(client.request("GET", f"/events/{selected_id}/history"))
            except ApiError as error:
                _show_error("Load event history", error)
    else:
        st.info("Load events to view the tenant-scoped event list.")


def _render_dead_letters(client: ApiClient) -> None:
    st.subheader("Dead-letter queue")
    if st.button("Load dead letters"):
        try:
            st.session_state["dead_letters"] = client.request("GET", "/dead-letters")
        except ApiError as error:
            _show_error("Load dead letters", error)
    dead_letters = st.session_state.get("dead_letters", [])
    if not dead_letters:
        st.info("No dead letters loaded.")
        return
    st.dataframe(dead_letters, use_container_width=True)
    event_ids = [item["event_id"] for item in dead_letters if item.get("event_id")]
    selected_id = st.selectbox("Dead-letter event to retry", event_ids)
    if st.button("Retry selected event"):
        try:
            result = client.request("POST", f"/events/{selected_id}/retry")
        except ApiError as error:
            _show_error("Retry event", error)
        else:
            st.success("Retry requested.")
            st.json(result)


def main() -> None:
    st.set_page_config(page_title="Event Processing Control", page_icon="⚙️", layout="wide")
    st.title("Event Processing Control")
    st.caption("Operate the existing FastAPI event-processing API.")
    client = _render_sidebar()
    overview, publish, pipelines, events, dead_letters = st.tabs(
        ["Overview", "Publish", "Pipelines", "Events", "Dead letters"]
    )
    with overview:
        _render_overview(client)
    with publish:
        _render_publish(client)
    with pipelines:
        _render_pipelines(client)
    with events:
        _render_events(client)
    with dead_letters:
        _render_dead_letters(client)


if __name__ == "__main__":
    main()
