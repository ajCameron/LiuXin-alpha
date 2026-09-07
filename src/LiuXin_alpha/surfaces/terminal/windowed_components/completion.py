"""Completion hints and cycling applied to the editable input buffer."""

from __future__ import annotations

from .contracts import WindowedState


class CompletionMixin(WindowedState):
    """Completion hints and cycling applied to the editable input buffer."""

    @staticmethod
    def _shared_prefix(values: tuple[str, ...]) -> str:
        if not values:
            return ""
        prefix = values[0]
        for value in values[1:]:
            limit = min(len(prefix), len(value))
            idx = 0
            while idx < limit and prefix[idx] == value[idx]:
                idx += 1
            prefix = prefix[:idx]
            if not prefix:
                break
        return prefix

    def _reset_completion(self, *, keep_hint: bool = False) -> None:
        self._completion_matches = ()
        self._completion_base_input = None
        self._completion_active_input = None
        self._completion_token_start = 0
        self._completion_token_end = 0
        self._completion_index = None
        if not keep_hint:
            self._completion_hint = None

    def _format_completion_hint(
        self, matches: tuple[str, ...], *, selected_index: int | None = None
    ) -> str:
        preview = list(matches[:6])
        preview_text = ", ".join(preview)
        remaining = len(matches) - len(preview)
        if remaining > 0:
            preview_text += f" (+{remaining} more)"
        if (
            selected_index is None
            or selected_index < 0
            or selected_index >= len(matches)
        ):
            return f"completion: {preview_text}"
        return f"completion: {matches[selected_index]} ({selected_index + 1}/{len(matches)}) | matches: {preview_text}"

    def _apply_completion_candidate(self, candidate: str) -> str:
        base = str(self._completion_base_input or self._current_input)
        start = max(0, int(self._completion_token_start))
        end = max(start, int(self._completion_token_end))
        completed = f"{base[:start]}{candidate}{base[end:]}"
        if not completed.endswith(" "):
            completed += " "
        return completed

    def _complete_current_input(self, *, direction: int = 1) -> bool:
        browser = self.browser
        if browser is None or not hasattr(browser, "command_completion_candidates"):
            self._reset_completion()
            return False

        if (
            self._completion_matches
            and self._completion_base_input is not None
            and self._completion_active_input == self._current_input
            and self._completion_index is not None
        ):
            next_index = (self._completion_index + int(direction)) % len(
                self._completion_matches
            )
            self._completion_index = next_index
            self._current_input = self._apply_completion_candidate(
                self._completion_matches[next_index]
            )
            self._completion_active_input = self._current_input
            self._completion_hint = self._format_completion_hint(
                self._completion_matches,
                selected_index=next_index,
            )
            self._render(force_status=False)
            return True

        try:
            completion = browser.command_completion_candidates(
                self._current_input, cursor=len(self._current_input)
            )
        except Exception:
            self._reset_completion()
            return False

        matches = tuple(
            str(candidate) for candidate in completion.candidates if str(candidate)
        )
        if not matches:
            self._reset_completion()
            return False

        self._reset_completion(keep_hint=True)
        shared_prefix = self._shared_prefix(matches)
        if len(matches) > 1 and len(shared_prefix) > len(str(completion.prefix)):
            self._current_input = (
                self._current_input[: completion.token_start]
                + shared_prefix
                + self._current_input[completion.token_end :]
            )
            self._completion_hint = self._format_completion_hint(matches)
            self._render(force_status=False)
            return True

        if len(matches) == 1:
            self._completion_base_input = self._current_input
            self._completion_token_start = int(completion.token_start)
            self._completion_token_end = int(completion.token_end)
            self._current_input = self._apply_completion_candidate(matches[0])
            self._completion_active_input = self._current_input
            self._completion_hint = self._format_completion_hint(
                matches, selected_index=0
            )
            self._render(force_status=False)
            return True

        self._completion_matches = matches
        self._completion_base_input = self._current_input
        self._completion_token_start = int(completion.token_start)
        self._completion_token_end = int(completion.token_end)
        self._completion_index = 0 if int(direction) >= 0 else (len(matches) - 1)
        self._current_input = self._apply_completion_candidate(
            matches[self._completion_index]
        )
        self._completion_active_input = self._current_input
        self._completion_hint = self._format_completion_hint(
            matches, selected_index=self._completion_index
        )
        self._render(force_status=False)
        return True
