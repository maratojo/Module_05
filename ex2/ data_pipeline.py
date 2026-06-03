from abc import ABC, abstractmethod
from typing import Any, Protocol


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
            raise Exception("Not data available")
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
