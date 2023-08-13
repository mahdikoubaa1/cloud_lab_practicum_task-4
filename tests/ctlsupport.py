from collections.abc import Callable
from testsupport import fail, warn


def check(msg: str,
          actual,
          expected,
          fail_hook: Callable = lambda: None
         ) -> None:
    """
    Checks if two values match. When mismatch, run fail_hook and exit(1)

    :param msg:       Test name
    :param actual:    Output to check
    :param expected:  Expected output
    :param fail_hook: run function when comparison failed; default: do nothing
    """

    if actual != expected:
        warn(f"expected: {expected}")
        warn(f"actual:   {actual}")
        try:
            if fail_hook:
                fail_hook()
        finally:
            fail(msg)


class InvalidResponseException(Exception):
    pass


class ctl_response:
    """
    response of ctl
    example format:

        Msg:    OK
        Key:    1
        Value:  OK
    """

    def __init__(self, msg: str, kvps: dict[str, str]):
        self.msg = msg
        self.kvps = kvps

    def __str__(self) -> str:
        return f"Msg: {self.msg}\nKvps: {self.kvps}"

    @classmethod
    def parse(cls, input: str):
        kvps = {}
        lines = list(filter(lambda l: l.startswith("Key:") or l.startswith("Value:"),
                            input.split("\n")))
        msg = list(filter(lambda l: l.startswith("Msg:"), input.split("\n")))
        msg = msg[0] if len(msg) > 0 else ""

        if len(lines) < 1:
            raise InvalidResponseException("No response found")

        try:
            it = iter(lines)
            for k, v in zip(it, it):
                name, key = map(lambda s: s.strip(), k.split(":"))
                if name != "Key":
                    raise InvalidResponseException(f"Invalid key name {name}, expected: Key")

                name, value = map(lambda s: s.strip(), v.split(":"))
                if name != "Value":
                    raise InvalidResponseException(f"Invalid key name {name}, expected: Value")
                kvps[key] = value
        except ValueError as err:
            print(err)
            raise InvalidResponseException("Fail to parse response")

        return cls(msg, kvps)
