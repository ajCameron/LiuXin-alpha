"""
Implement expression identity fields independently of the editable relation bundle.

ExpressionIdentity stores intrinsic form, language, title, extent and lifecycle
data. Flags are normalized locally; other values are retained without database
lookup.

Example:
    >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
    >>> expression.expression_id
    2
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Mapping, Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.expression_containers.expression_identity_api import (
    ExpressionFlags,
    ExpressionIdentityAPI,
)
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)


def _coerce_expression_flags(value: Iterable[str] | str | None) -> ExpressionFlags:
    """
    Normalize comma-delimited text or an iterable into an ordered tuple of distinct flags.

    Tokens are stringified and stripped, blanks are removed, and duplicates are compared
    case-sensitively. None input produces an empty tuple.

    Example:
        >>> _coerce_expression_flags('draft, reviewed, draft, DRAFT, ')
        ('draft', 'reviewed', 'DRAFT')


    :param value: Comma-delimited text, iterable of flag values, or None.
    :return: Tuple of nonempty flags in first-seen order.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        tokens = value.split(",")
    else:
        tokens = value

    flags: list[str] = []
    seen: set[str] = set()
    for raw_token in tokens:
        token = str(raw_token).strip()
        if token and token not in seen:
            flags.append(token)
            seen.add(token)
    return tuple(flags)


def _serialize_expression_flags(value: ExpressionFlags) -> str | None:
    """
    Join stored flags with commas without additional normalization or escaping.

    Example:
        >>> _serialize_expression_flags(('draft', 'reviewed'))
        'draft,reviewed'
        >>> _serialize_expression_flags(()) is None
        True


    :param value: Normalized tuple of expression flags.
    :return: Comma-delimited text, or None for an empty tuple.
    """
    if not value:
        return None
    return ",".join(value)


class ExpressionIdentity(ExpressionIdentityAPI):
    """
    Store intrinsic expression metadata with an id that can be set while unset.

    Flags are normalized into an ordered tuple. Other fields remain editable as
    supplied; once the expression id is non-None the public setter rejects all further
    assignments.

    Example:
        >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
        >>> expression.expression_label
        'English text'
    """
    def __init__(
        self,
        *,
        expression_id: Optional[int] = None,
        expression_work_id: Optional[int] = None,
        expression_type: Optional[str] = None,
        expression_language_id: Optional[int] = None,
        expression_label: Optional[str] = None,
        expression_title_override: Optional[str] = None,
        expression_subtitle: Optional[str] = None,
        expression_flags: Iterable[str] | str | None = None,
        expression_status: Optional[str] = None,
        expression_original_date: Optional[str] = None,
        expression_original_copyright_date: Optional[str] = None,
        expression_year: Optional[int] = None,
        expression_wordcount: Optional[int] = None,
        expression_nominal_duration_seconds: Optional[int] = None,
        expression_created_timestamp_ep_k: Optional[int] = None,
        expression_modified_timestamp_ep_k: Optional[int] = None,
        expression_scratch: Optional[str] = None,
    ) -> None:
        """
        Store intrinsic expression fields and normalize flags.

        No scalar coercion, foreign-key resolution or persistence is performed.

        Example:
            >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
            >>> expression.expression_work_id is None
            True


        :param expression_id: Optional expression row id; a non-None value locks the public
            id setter.
        :param expression_work_id: Optional parent-work hint, retained as supplied.
        :param expression_type: Optional expression form or type, retained as supplied.
        :param expression_language_id: Optional language row id, retained as supplied.
        :param expression_label: Optional display label, retained as supplied.
        :param expression_title_override: Optional expression-specific title override,
            retained as supplied.
        :param expression_subtitle: Optional subtitle, retained as supplied.
        :param expression_flags: Comma-delimited text or iterable of flags to normalize;
            None starts empty.
        :param expression_status: Optional status text, retained as supplied.
        :param expression_original_date: Optional original date text, retained as supplied.
        :param expression_original_copyright_date: Optional original copyright date text,
            retained as supplied.
        :param expression_year: Optional year value, retained as supplied.
        :param expression_wordcount: Optional word count, retained as supplied.
        :param expression_nominal_duration_seconds: Optional nominal duration in seconds,
            retained as supplied.
        :param expression_created_timestamp_ep_k: Optional creation timestamp in ep_k form,
            retained as supplied.
        :param expression_modified_timestamp_ep_k: Optional modification timestamp in ep_k
            form, retained as supplied.
        :param expression_scratch: Optional scratch text, retained as supplied.
        :return: None.
        """
        self._expression_id = expression_id
        self._expression_work_id = expression_work_id
        self._expression_type = expression_type
        self._expression_language_id = expression_language_id
        self._expression_label = expression_label
        self._expression_title_override = expression_title_override
        self._expression_subtitle = expression_subtitle
        self._expression_flags = _coerce_expression_flags(expression_flags)
        self._expression_status = expression_status
        self.expression_original_date = expression_original_date
        self.expression_original_copyright_date = expression_original_copyright_date
        self.expression_year = expression_year
        self.expression_wordcount = expression_wordcount
        self.expression_nominal_duration_seconds = expression_nominal_duration_seconds
        self.expression_created_timestamp_ep_k = expression_created_timestamp_ep_k
        self.expression_modified_timestamp_ep_k = expression_modified_timestamp_ep_k
        self.expression_scratch = expression_scratch

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "ExpressionIdentity":
        """
        Build an identity from recognized expression-prefixed columns.

        Missing columns become None and unrelated keys are ignored. Construction normalizes
        flags but retains the other values without coercion.

        Example:
            >>> expression = ExpressionIdentity.from_mapping({'expression_id': 2, 'expression_flags': 'draft, draft', 'unused': 9})
            >>> expression.expression_id, expression.expression_flags
            (2, ('draft',))


        :param row: Mapping supplying optional expression identity columns.
        :return: New identity instance of the requested class.
        """
        return cls(**{k: row.get(k) for k in [
            "expression_id", "expression_work_id", "expression_type", "expression_language_id",
            "expression_label", "expression_title_override", "expression_subtitle", "expression_flags",
            "expression_status", "expression_original_date", "expression_original_copyright_date",
            "expression_year", "expression_wordcount", "expression_nominal_duration_seconds",
            "expression_created_timestamp_ep_k", "expression_modified_timestamp_ep_k", "expression_scratch",
        ]})

    def to_mapping(self) -> dict[str, Any]:
        """
        Serialize intrinsic fields and encode flags as comma-delimited storage text.

        Example:
            >>> expression = ExpressionIdentity(expression_flags=('draft', 'draft'))
            >>> expression.to_mapping()['expression_flags']
            'draft'


        :return: New dictionary retaining None values; empty flags serialize as None.
        """
        return {
            "expression_id": self.expression_id,
            "expression_work_id": self.expression_work_id,
            "expression_type": self.expression_type,
            "expression_language_id": self.expression_language_id,
            "expression_label": self.expression_label,
            "expression_title_override": self.expression_title_override,
            "expression_subtitle": self.expression_subtitle,
            "expression_flags": _serialize_expression_flags(self.expression_flags),
            "expression_status": self.expression_status,
            "expression_original_date": self.expression_original_date,
            "expression_original_copyright_date": self.expression_original_copyright_date,
            "expression_year": self.expression_year,
            "expression_wordcount": self.expression_wordcount,
            "expression_nominal_duration_seconds": self.expression_nominal_duration_seconds,
            "expression_created_timestamp_ep_k": self.expression_created_timestamp_ep_k,
            "expression_modified_timestamp_ep_k": self.expression_modified_timestamp_ep_k,
            "expression_scratch": self.expression_scratch,
        }

    def __str__(self) -> str:
        """
        Render a compact diagnostic summary emphasizing ids, title, label and type.

        Example:
            >>> expression = ExpressionIdentity(expression_id=2, expression_label='English text')
            >>> 'English text' in str(expression)
            True


        :return: Human-readable expression identity summary.
        """
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=("expression_id", "expression_work_id"),
            display_keys=(
                "expression_title_override",
                "expression_label",
                "expression_type",
            ),
        )

    @property
    def expression_id(self) -> Optional[int]:
        """
        Return the stored expression row id.

        Example:
            >>> expression = ExpressionIdentity(expression_id=2)
            >>> expression.expression_id
            2


        :return: Expression row id, or None when unset.
        """
        return self._expression_id
    @expression_id.setter
    def expression_id(self, value: Optional[int]) -> None:
        """
        Assign an expression id only while the stored id is None.

        A non-None stored id causes AttributeError even for the same value. Assigning None
        while unset leaves the setter available.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_id = 2
            >>> expression.expression_id = 2
            Traceback (most recent call last):
            ...
            AttributeError: Expression id is already set.


        :param value: New expression row id, or None to leave or mark it unset.
        :return: None.
        """
        if self._expression_id is None: self._expression_id = value
        else: raise AttributeError("Expression id is already set.")
    @property
    def expression_work_id(self) -> Optional[int]:
        """
        Return the stored parent-work hint.

        Example:
            >>> expression = ExpressionIdentity(expression_work_id=1)
            >>> expression.expression_work_id
            1


        :return: Parent-work hint, or None when unset.
        """
        return self._expression_work_id
    @expression_work_id.setter
    def expression_work_id(self, value: Optional[int]) -> None:
        """
        Replace the parent-work hint without coercion or lookup.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_work_id = 1
            >>> expression.expression_work_id
            1


        :param value: New parent-work hint, or None to leave or mark it unset.
        :return: None.
        """
        self._expression_work_id = value
    @property
    def expression_type(self) -> Optional[str]:
        """
        Return the stored expression form or type.

        Example:
            >>> expression = ExpressionIdentity(expression_type='text')
            >>> expression.expression_type
            'text'


        :return: Expression form or type, or None when unset.
        """
        return self._expression_type
    @expression_type.setter
    def expression_type(self, value: Optional[str]) -> None:
        """
        Replace the expression form or type without coercion or lookup.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_type = 'text'
            >>> expression.expression_type
            'text'


        :param value: New expression form or type, or None to leave or mark it unset.
        :return: None.
        """
        self._expression_type = value
    @property
    def expression_language_id(self) -> Optional[int]:
        """
        Return the stored language row id.

        Example:
            >>> expression = ExpressionIdentity(expression_language_id=3)
            >>> expression.expression_language_id
            3


        :return: Language row id, or None when unset.
        """
        return self._expression_language_id
    @expression_language_id.setter
    def expression_language_id(self, value: Optional[int]) -> None:
        """
        Replace the language row id without coercion or lookup.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_language_id = 3
            >>> expression.expression_language_id
            3


        :param value: New language row id, or None to leave or mark it unset.
        :return: None.
        """
        self._expression_language_id = value
    @property
    def expression_label(self) -> Optional[str]:
        """
        Return the stored display label.

        Example:
            >>> expression = ExpressionIdentity(expression_label='English text')
            >>> expression.expression_label
            'English text'


        :return: Display label, or None when unset.
        """
        return self._expression_label
    @expression_label.setter
    def expression_label(self, value: Optional[str]) -> None:
        """
        Replace the display label without coercion or lookup.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_label = 'English text'
            >>> expression.expression_label
            'English text'


        :param value: New display label, or None to leave or mark it unset.
        :return: None.
        """
        self._expression_label = value
    @property
    def expression_title_override(self) -> Optional[str]:
        """
        Return the stored expression-specific title override.

        Example:
            >>> expression = ExpressionIdentity(expression_title_override='Notes')
            >>> expression.expression_title_override
            'Notes'


        :return: Expression-specific title override, or None when unset.
        """
        return self._expression_title_override
    @expression_title_override.setter
    def expression_title_override(self, value: Optional[str]) -> None:
        """
        Replace the expression-specific title override without coercion or lookup.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_title_override = 'Notes'
            >>> expression.expression_title_override
            'Notes'


        :param value: New expression-specific title override, or None to leave or mark it
            unset.
        :return: None.
        """
        self._expression_title_override = value
    @property
    def expression_subtitle(self) -> Optional[str]:
        """
        Return the stored subtitle.

        Example:
            >>> expression = ExpressionIdentity(expression_subtitle='An edition')
            >>> expression.expression_subtitle
            'An edition'


        :return: Subtitle, or None when unset.
        """
        return self._expression_subtitle
    @expression_subtitle.setter
    def expression_subtitle(self, value: Optional[str]) -> None:
        """
        Replace the subtitle without coercion or lookup.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_subtitle = 'An edition'
            >>> expression.expression_subtitle
            'An edition'


        :param value: New subtitle, or None to leave or mark it unset.
        :return: None.
        """
        self._expression_subtitle = value
    @property
    def expression_flags(self) -> ExpressionFlags:
        """
        Return normalized flags in first-seen order.

        Example:
            >>> ExpressionIdentity(expression_flags='draft, draft').expression_flags
            ('draft',)


        :return: Tuple of distinct, case-sensitive flag strings.
        """
        return self._expression_flags
    @expression_flags.setter
    def expression_flags(self, value: Iterable[str] | str | None) -> None:
        """
        Replace flags after stripping blanks and removing exact duplicates.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_flags = [' draft ', '', 'draft', 'DRAFT']
            >>> expression.expression_flags
            ('draft', 'DRAFT')


        :param value: Comma-delimited text, iterable of flag values, or None to clear flags.
        :return: None.
        """
        self._expression_flags = _coerce_expression_flags(value)
    @property
    def expression_status(self) -> Optional[str]:
        """
        Return the stored status text.

        Example:
            >>> expression = ExpressionIdentity(expression_status='active')
            >>> expression.expression_status
            'active'


        :return: Status text, or None when unset.
        """
        return self._expression_status
    @expression_status.setter
    def expression_status(self, value: Optional[str]) -> None:
        """
        Replace the status text without coercion or lookup.

        Example:
            >>> expression = ExpressionIdentity()
            >>> expression.expression_status = 'active'
            >>> expression.expression_status
            'active'


        :param value: New status text, or None to leave or mark it unset.
        :return: None.
        """
        self._expression_status = value


__all__ = ["ExpressionIdentity"]
