"""Sequence-numbered output delivery for reliable output transport.

Manages held data and sequence counter so that output drained from buffers
is not lost if an RPC response fails to reach the host.
"""

from collections.abc import Sequence


class SequencedDelivery:
    """Track held output and sequence numbers for reliable delivery.

    The host sends ``ack_seq`` on every RPC to confirm receipt of the
    previous response.  When ``ack_seq >= _seq``, held data is discarded.
    Otherwise held data is prepended to the next response.
    """

    def __init__(self, fields: Sequence[str]) -> None:
        self._seq: int = 0
        self._held: dict[str, str] = {f: "" for f in fields}

    def deliver(
        self, ack_seq: int, data: dict[str, str]
    ) -> tuple[int, dict[str, str]]:
        """Accept fresh data, combine with any unacked held data.

        Args:
            ack_seq: Host's last successfully received seq (0 = nothing received).
            data: Freshly drained output keyed by field name.

        Returns:
            ``(new_seq, combined_data)`` for the response.
        """
        if ack_seq >= self._seq:
            self._held = {f: "" for f in self._held}

        combined = {f: self._held[f] + data.get(f, "") for f in self._held}

        self._seq += 1
        self._held = combined

        return (self._seq, combined)
