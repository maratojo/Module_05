from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    name: str = ""

    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0
        self._processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if len(self._storage) == 0:
            raise Exception("No data available")
        value = self._storage.pop(0)
        result = (self._rank, value)
        self._rank += 1
        return result


class NumericProcessor(DataProcessor):
    name = "Numeric Processor"

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
            data: Any) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        if isinstance(data, int | float):
            self._storage.append(str(data))
            self._processed += 1

        else:
            for item in data:
                self._storage.append(str(item))
                self._processed += 1


class TextProcessor(DataProcessor):
    name = "Text Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")
        if isinstance(data, str):
            self._storage.append(data)
            self._processed += 1

        else:
            for item in data:
                self._storage.append(item)
                self._processed += 1


class LogProcessor(DataProcessor):
    name = "Log Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            if "log_level" not in data or "log_message" not in data:
                return False
            if not isinstance(data["log_level"], str):
                return False
            if not isinstance(data["log_message"], str):
                return False
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    return False
                elif isinstance(item, dict):
                    if "log_level" not in item \
                                or "log_message" not in item:
                        return False
                    if not isinstance(item["log_level"], str):
                        return False
                    if not isinstance(item["log_message"], str):
                        return False
            return True
        return False

    def ingest(
            self,
            data: dict[str, str] |
            list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")
        if isinstance(data, dict):
            format = (f"{data['log_level']}: {data['log_message']}")
            self._storage.append(format)
            self._processed += 1
        else:
            for item in data:
                format = (f"{item['log_level']}: {item['log_message']}")
                self._storage.append(format)
                self._processed += 1


class DataStream:
    def __init__(self) -> None:
        self._register: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._register.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            found = False
            for processor in self._register:
                if processor.validate(element):
                    processor.ingest(element)
                    found = True
                    break
            if not found:
                print("DataStream error- Can't process element in stream:", element)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._register:
            print("No processor found, no data")
            return
        for proc in self._register:
            print(f"{proc.name}: total {proc._processed} items processed, remaining {len(proc._storage)} on processor")


if __name__ == "__main__":
    print("=== Code Nexus- Data Stream ===\n")
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    print("\nRegistering Numeric Processor\n")
    num = NumericProcessor()
    stream.register_processor(num)
    batch = ['Hello world', [3.14, -1, 2.71], [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, {'log_level': 'INFO', 'log_message': 'User wil isconnected'}], 42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print("\nRegistering other data processors")
    print("Send the same batch again")
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(text)
    stream.register_processor(log)
    stream.process_stream(batch)
    stream.print_processors_stats()
    print("Consume some elements from the data processors: Numeric 3, Text 2, Log 1\n")
    for _ in range(3):
        num.output()
    for _ in range(2):
        text.output()
    log.output()
    stream.print_processors_stats()
