from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:

        if len(self._storage) == 0:
            raise ValueError("No data available")

        value = self._storage.pop(0)

        result = (self._rank, value)

        self._rank += 1

        return result


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:

        if isinstance(data, int | float):
            return True

        if isinstance(data, list):

            for item in data:

                if not isinstance(item, int | float):
                    return False

            return True

        return False

    def ingest(
        self,
        data: int | float | list[int | float]
    ) -> None:

        if not self.validate(data):
            raise TypeError("Improper numeric data")

        if isinstance(data, int | float):
            self._storage.append(str(data))

        else:

            for item in data:
                self._storage.append(str(item))


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:

        if isinstance(data, str):
            return True

        if isinstance(data, list):

            for item in data:

                if not isinstance(item, str):
                    return False

            return True

        return False

    def ingest(
        self,
        data: str | list[str]
    ) -> None:

        if not self.validate(data):
            raise TypeError("Improper text data")

        if isinstance(data, str):
            self._storage.append(data)

        else:

            for item in data:
                self._storage.append(item)


class LogProcessor(DataProcessor):

    def _is_valid_log(self, data: Any) -> bool:

        if not isinstance(data, dict):
            return False

        if "log_level" not in data:
            return False

        if "log_message" not in data:
            return False

        if not isinstance(data["log_level"], str):
            return False

        if not isinstance(data["log_message"], str):
            return False

        return True

    def validate(self, data: Any) -> bool:

        if self._is_valid_log(data):
            return True

        if isinstance(data, list):

            for item in data:

                if not self._is_valid_log(item):
                    return False

            return True

        return False

    def ingest(
        self,
        data: dict[str, str] | list[dict[str, str]]
    ) -> None:

        if not self.validate(data):
            raise TypeError("Improper log data")

        if isinstance(data, dict):

            formatted = (
                f"{data['log_level']}: "
                f"{data['log_message']}"
            )

            self._storage.append(formatted)

        else:

            for item in data:

                formatted = (
                    f"{item['log_level']}: "
                    f"{item['log_message']}"
                )

                self._storage.append(formatted)


if __name__ == "__main__":

    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")

    numeric = NumericProcessor()

    print(
        "Trying to validate input '42':",
        numeric.validate(42)
    )

    print(
        "Trying to validate input 'Hello':",
        numeric.validate("Hello")
    )

    print(
        "Test invalid ingestion of string "
        "'foo' without prior validation:"
    )

    try:
        numeric.ingest("foo")  # type: ignore

    except Exception as e:
        print("Got exception:", e)

    print("Processing data: [1, 2, 3, 4, 5]")

    numeric.ingest([1, 2, 3, 4, 5])

    print("Extracting 3 values...")

    for _ in range(3):

        rank, value = numeric.output()

        print(f"Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")

    text = TextProcessor()

    print(
        "Trying to validate input '42':",
        text.validate(42)
    )

    print(
        "Processing data: "
        "['Hello', 'Nexus', 'World']"
    )

    text.ingest(["Hello", "Nexus", "World"])

    print("Extracting 1 value...")

    rank, value = text.output()

    print(f"Text value {rank}: {value}")

    print("\nTesting Log Processor...")

    log = LogProcessor()

    print(
        "Trying to validate input 'Hello':",
        log.validate("Hello")
    )

    logs = [
        {
            "log_level": "NOTICE",
            "log_message": "Connection to server"
        },
        {
            "log_level": "ERROR",
            "log_message": "Unauthorized access!!"
        }
    ]

    print("Processing data:", logs)

    log.ingest(logs)

    print("Extracting 2 values...")

    for _ in range(2):

        rank, value = log.output()

        print(f"Log entry {rank}: {value}")