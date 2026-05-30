from __future__ import annotations

import json
from base64 import b64decode, b64encode
from typing import Literal

import itsdangerous
from starlette.datastructures import MutableHeaders, Secret
from starlette.middleware.sessions import Session
from starlette.requests import HTTPConnection
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class RememberMeSessionMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        secret_key: str | Secret,
        session_cookie: str = "session",
        remember_max_age: int = 365 * 24 * 60 * 60,
        path: str = "/",
        same_site: Literal["lax", "strict", "none"] = "lax",
        https_only: bool = False,
        domain: str | None = None,
    ) -> None:
        self.app = app
        self.signer = itsdangerous.TimestampSigner(str(secret_key))
        self.session_cookie = session_cookie
        self.remember_max_age = remember_max_age
        self.path = path
        self.security_flags = "httponly; samesite=" + same_site
        if https_only:
            self.security_flags += "; secure"
        if domain is not None:
            self.security_flags += f"; domain={domain}"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        connection = HTTPConnection(scope)
        initial_session_was_empty = True
        if self.session_cookie in connection.cookies:
            data = connection.cookies[self.session_cookie].encode("utf-8")
            try:
                data = self.signer.unsign(data, max_age=self.remember_max_age)
                scope["session"] = Session(json.loads(b64decode(data)))
                initial_session_was_empty = False
            except itsdangerous.BadSignature:
                scope["session"] = Session()
        else:
            scope["session"] = Session()

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                session: Session = scope["session"]
                headers = MutableHeaders(scope=message)
                if session.accessed:
                    headers.add_vary_header("Cookie")
                if session.modified and session:
                    max_age = (
                        f"Max-Age={self.remember_max_age}; "
                        if session.get("remember_me")
                        else ""
                    )
                    data = b64encode(json.dumps(session).encode("utf-8"))
                    data = self.signer.sign(data)
                    headers.append(
                        "Set-Cookie",
                        (
                            f"{self.session_cookie}={data.decode('utf-8')}; "
                            f"path={self.path}; {max_age}{self.security_flags}"
                        ),
                    )
                elif session.modified and not initial_session_was_empty:
                    headers.append(
                        "Set-Cookie",
                        (
                            f"{self.session_cookie}=null; path={self.path}; "
                            "expires=Thu, 01 Jan 1970 00:00:00 GMT; "
                            f"{self.security_flags}"
                        ),
                    )
            await send(message)

        await self.app(scope, receive, send_wrapper)
