from abc import ABC, abstractmethod
from typing import Any, Dict

class Task(ABC):
    """
    모든 Task가 상속받아야 하는 추상 기본 클래스입니다.
    Task는 오직 하나의 execute 메서드만 가져야 하며, 
    이전 Task의 결과를 받아 처리하거나 다음 Task로 결과를 전달합니다.
    """
    def __init__(self):
        # Task 실행 상태 관리를 위한 기본 초기화
        pass

    @abstractmethod
    def execute(self, input_data: Any = None) -> Dict[str, Any]:
        """
        Task의 핵심 로직을 실행합니다. 
        :param input_data: 이전 Task로부터 전달받은 데이터 (Pipeline에서 사용)
        :return: 다음 Task로 전달할 결과 딕셔너리 (상태, 데이터 포함)
        """
        raise NotImplementedError("Subclasses must implement the execute method.")
