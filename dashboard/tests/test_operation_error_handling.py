import asyncio
import json

from sqlalchemy.exc import IntegrityError
from starlette.datastructures import FormData

from dashboard.app.routes import operations, settings


class _Session:
    def __init__(self) -> None:
        self.rolled_back = False

    async def rollback(self) -> None:
        self.rolled_back = True


def _integrity_error() -> IntegrityError:
    return IntegrityError("statement", {}, RuntimeError("constraint failure"))


def test_operation_integrity_error_rolls_back_and_returns_conflict() -> None:
    session = _Session()

    async def fail():
        raise _integrity_error()

    response = asyncio.run(operations._result(session, fail()))

    assert response.status_code == 409
    assert session.rolled_back is True
    assert json.loads(response.body)["ok"] is False


def test_manager_integrity_error_rolls_back_and_redirects(monkeypatch) -> None:
    session = _Session()

    class Request:
        async def form(self):
            return FormData(
                {
                    "guild_id": "100",
                    "discord_user_id": "200",
                    "scope_code": "5",
                }
            )

    async def allow(*_args, **_kwargs):
        return None

    async def fail(*_args, **_kwargs):
        raise _integrity_error()

    monkeypatch.setattr(
        settings,
        "_require_operational_assignment_configuration",
        allow,
    )
    monkeypatch.setattr(settings.settings_store, "add_assignment", fail)

    response = asyncio.run(settings.save_manager(Request(), session))

    assert response.status_code == 303
    assert session.rolled_back is True
    assert "error=" in response.headers["location"]
