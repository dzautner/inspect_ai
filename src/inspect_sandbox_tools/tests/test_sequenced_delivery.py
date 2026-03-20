"""Tests for SequencedDelivery — pure logic, no async, no mocks."""

from inspect_sandbox_tools._remote_tools._exec_remote._sequenced_delivery import (
    SequencedDelivery,
)


def test_first_delivery() -> None:
    sd = SequencedDelivery(["stdout", "stderr"])
    seq, out = sd.deliver(0, {"stdout": "hello", "stderr": "err"})
    assert seq == 1
    assert out == {"stdout": "hello", "stderr": "err"}


def test_normal_ack_flow() -> None:
    sd = SequencedDelivery(["stdout", "stderr"])

    seq1, out1 = sd.deliver(0, {"stdout": "A", "stderr": ""})
    assert seq1 == 1
    assert out1 == {"stdout": "A", "stderr": ""}

    # Host acks seq 1
    seq2, out2 = sd.deliver(1, {"stdout": "B", "stderr": "e"})
    assert seq2 == 2
    assert out2 == {"stdout": "B", "stderr": "e"}


def test_retransmit_prepends_held() -> None:
    sd = SequencedDelivery(["stdout", "stderr"])

    seq1, _ = sd.deliver(0, {"stdout": "A", "stderr": "x"})
    assert seq1 == 1

    # Host did NOT receive seq 1, sends ack_seq=0 again
    seq2, out2 = sd.deliver(0, {"stdout": "B", "stderr": "y"})
    assert seq2 == 2
    assert out2 == {"stdout": "AB", "stderr": "xy"}


def test_multiple_retransmits_accumulate() -> None:
    sd = SequencedDelivery(["out"])

    sd.deliver(0, {"out": "A"})   # seq=1
    sd.deliver(0, {"out": "B"})   # seq=2, held="AB"
    seq3, out3 = sd.deliver(0, {"out": "C"})  # seq=3, held="ABC"
    assert seq3 == 3
    assert out3 == {"out": "ABC"}


def test_empty_fresh_on_retransmit() -> None:
    sd = SequencedDelivery(["stdout"])

    sd.deliver(0, {"stdout": "data"})  # seq=1
    seq2, out2 = sd.deliver(0, {"stdout": ""})  # retransmit, no new data
    assert seq2 == 2
    assert out2 == {"stdout": "data"}


def test_empty_held_and_empty_fresh() -> None:
    sd = SequencedDelivery(["a", "b"])
    seq, out = sd.deliver(0, {"a": "", "b": ""})
    assert seq == 1
    assert out == {"a": "", "b": ""}


def test_ack_seq_greater_than_seq_clears_held() -> None:
    """Defensive: ack_seq > _seq treated same as ==."""
    sd = SequencedDelivery(["stdout"])

    sd.deliver(0, {"stdout": "A"})  # seq=1
    # ack_seq=5 > _seq=1 — should still clear held
    seq2, out2 = sd.deliver(5, {"stdout": "B"})
    assert seq2 == 2
    assert out2 == {"stdout": "B"}


def test_single_field() -> None:
    sd = SequencedDelivery(["output"])
    seq, out = sd.deliver(0, {"output": "x"})
    assert seq == 1
    assert out == {"output": "x"}


def test_three_fields() -> None:
    sd = SequencedDelivery(["a", "b", "c"])
    seq, out = sd.deliver(0, {"a": "1", "b": "2", "c": "3"})
    assert seq == 1
    assert out == {"a": "1", "b": "2", "c": "3"}


def test_missing_field_in_data_defaults_empty() -> None:
    sd = SequencedDelivery(["stdout", "stderr"])
    seq, out = sd.deliver(0, {"stdout": "hello"})
    assert seq == 1
    assert out == {"stdout": "hello", "stderr": ""}


def test_ack_then_retransmit_then_ack() -> None:
    """Full cycle: normal → lost → retransmit → ack."""
    sd = SequencedDelivery(["o"])

    # Normal
    sd.deliver(0, {"o": "A"})  # seq=1
    sd.deliver(1, {"o": "B"})  # seq=2, acked 1

    # Response for seq=2 lost
    sd.deliver(1, {"o": "C"})  # seq=3, held="BC"

    # Host finally acks seq=3
    seq4, out4 = sd.deliver(3, {"o": "D"})
    assert seq4 == 4
    assert out4 == {"o": "D"}
