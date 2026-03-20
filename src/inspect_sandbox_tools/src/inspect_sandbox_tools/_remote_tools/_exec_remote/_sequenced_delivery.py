"""Sequence-numbered output delivery for reliable output transport.

Manages held chunks and sequence counter so that output drained from buffers
is not lost if an RPC response fails to reach the host.
"""


class SequencedDelivery[T]:
    """Track held chunks and sequence numbers for reliable delivery.

    Two-step usage per RPC round-trip:

    1. ``push(chunk)`` — record new output.
    2. ``collect(ack_seq)`` — discard chunks the host has confirmed,
       return everything still unacked with the current seq.

    Example — normal flow::

        sd = SequencedDelivery[str]()

        sd.push("A")
        seq, chunks = sd.collect(0)  # seq=1, chunks=["A"]

        sd.push("B")
        seq, chunks = sd.collect(1)  # seq=2, chunks=["B"]

    Example — retransmit (response lost)::

        sd = SequencedDelivery[str]()

        sd.push("A")
        sd.collect(0)                # seq=1, ["A"] — response lost

        sd.push("B")
        seq, chunks = sd.collect(0)  # seq=2, ["A", "B"]
    """

    def __init__(self) -> None:
        self._seq: int = 0
        self._held: list[tuple[int, T]] = []  # (seq, chunk) pairs

    def push(self, chunk: T) -> None:
        """Append a chunk."""
        self._seq += 1
        self._held.append((self._seq, chunk))

    def collect(self, ack_seq: int) -> tuple[int, list[T]]:
        """Discard acked chunks and return the current seq plus what's left.

        Args:
            ack_seq: Host's last successfully received seq (0 = nothing received).

        Returns:
            ``(seq, chunks)`` — current sequence number and copy of unacked chunks.
        """
        self._held = [(s, c) for s, c in self._held if s > ack_seq]
        return (self._seq, [c for _, c in self._held])
