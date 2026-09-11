"""
Apply browser completion results to the curses input buffer and its status hint.

Completion first expands a shared prefix when possible, then selects or cycles
whole candidates. The driver owns the buffer, cached token span, and redraw hook.
"""

from __future__ import annotations

from .contracts import WindowedState


class CompletionMixin(WindowedState):
    """
    Manage completion state for a composed driver with an attached browser.

    Input edits invalidate cycling when they no longer match the active completed
    buffer. Lookup failures clear completion state without replacing user input.

    Example:
        >>> CompletionMixin._shared_prefix(("browse", "browser"))
        'browse'
    """

    @staticmethod
    def _shared_prefix(values: tuple[str, ...]) -> str:
        """
        Find the longest case-sensitive character prefix shared by all candidates.

        Example:
            >>> CompletionMixin._shared_prefix(("show", "shell", "schema"))
            's'
            >>> CompletionMixin._shared_prefix(())
            ''


        :param values: Completion candidates whose leading characters are compared.
        :return: Common prefix, or an empty string for no candidates or no overlap.
        """
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
        """
        Clear cached candidates, token bounds, and cycling state without editing input.

        Example:
            >>> driver._reset_completion(keep_hint=True)  # doctest: +SKIP


        :param keep_hint: Preserve the displayed hint while clearing the cycle cache.
        :return: ``None``; completion state on the driver is reset in place.
        """
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
        """
        Describe up to six matches and optionally identify the selected candidate.

        Additional matches are counted rather than listed. An absent or out-of-range
        selection produces only the preview; displayed selection numbers are one-based.

        Example:
            >>> driver._format_completion_hint(  # doctest: +SKIP
            ...     ("show", "schema"), selected_index=1
            ... )
            'completion: schema (2/2) | matches: show, schema'


        :param matches: Ordered candidates to summarize without modifying their text.
        :param selected_index: Optional zero-based index of the active candidate.
        :return: Status-hint text, including a selected/total count when applicable.
        """
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
        """
        Replace the cached token span and ensure the resulting input ends with a space.

        Use the saved base input when truthy, otherwise the current buffer. Bounds
        are clamped to nonnegative order and sliced normally. Any suffix after the
        replaced span is retained; the space is appended to the complete line, not
        inserted immediately after the candidate.

        Example:
            >>> completed = driver._apply_completion_candidate("browse")  # doctest: +SKIP


        :param candidate: Complete token text to splice into the saved input.
        :return: New input string; driver state is not changed by this helper.
        """
        base = str(self._completion_base_input or self._current_input)
        start = max(0, int(self._completion_token_start))
        end = max(start, int(self._completion_token_end))
        completed = f"{base[:start]}{candidate}{base[end:]}"
        if not completed.endswith(" "):
            completed += " "
        return completed

    def _complete_current_input(self, *, direction: int = 1) -> bool:
        """
        Expand or select completion at the input end, or cycle an active selection.

        An unchanged active selection cycles by ``direction`` modulo the match
        count. A fresh lookup first expands a longer shared prefix; otherwise it
        applies the sole match or starts a cycle at the first/last match according
        to the direction's sign. Applied completions update the hint and redraw.

        An unavailable browser, lookup exception, or empty result clears the cache
        and returns ``False`` without editing input. Invalid result conversion and
        drawing errors are not caught. ``True`` means completion was handled, not
        necessarily that its resulting text differs from the previous buffer.

        Example:
            >>> handled = driver._complete_current_input(direction=-1)  # doctest: +SKIP


        :param direction: Integer cycle step; negative values start a fresh cycle last.
        :return: Whether a prefix expansion or candidate selection was handled.
        """
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
