"""Retain uncertain external effects without repeating a command after response loss."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.core import CoreRuntime
from LiuXin_alpha.core.commands import CoreCommand
from LiuXin_alpha.core.errors import CoreError
from tests.databases.caches.test_metadata_api import metadata_db as metadata_db
from tests.databases.caches.test_metadata_api import (
    metadata_template as metadata_template,
)


@pytest.mark.parametrize("interrupted", [False, True])
def test_external_outcome_replay_or_held_interruption(
    metadata_db, tmp_path, interrupted
):
    path = tmp_path / "effect"
    calls = []

    def effect(runtime, command):
        calls.append(1)
        path.write_text("published")
        if interrupted:
            raise SystemExit("executor disappeared after publication")
        return {"published": str(path)}

    with CoreRuntime(library=SimpleNamespace(database=metadata_db)) as runtime:
        runtime.register_command_handler(
            "test.effect", effect, recovery="observed", remote_capabilities=("admin",)
        )
        command = CoreCommand("test.effect", {}, command_id="external-effect")
        if interrupted:
            with pytest.raises(SystemExit):
                runtime.execute_command(command)
            with pytest.raises(CoreError) as held:
                runtime.execute_command(command)
            assert held.value.code == "command_unresolved"
        else:
            assert (
                runtime.execute_command(command).result
                == runtime.execute_command(command).result
            )
        assert calls == [1] and path.read_text() == "published"
        outcome = runtime.query("commands.outcome", {"command_id": command.command_id})
        assert outcome["state"] == ("in_progress" if interrupted else "succeeded")
        with pytest.raises(CoreError) as conflict:
            runtime.execute_command(
                CoreCommand(
                    "test.effect", {"changed": True}, command_id=command.command_id
                )
            )
        assert conflict.value.code == "command_conflict"
