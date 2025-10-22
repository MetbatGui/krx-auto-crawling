from abc import ABC, abstractmethod
from typing import Dict, Any, List
from core.tasks.base_task import Task

class Pipeline(ABC):
    """
    모든 파이프라인의 추상 기본 클래스입니다. 
    Task 객체들의 리스트를 순차적으로 실행하여 데이터 흐름을 관리하는 템플릿 역할을 합니다.
    """
    def __init__(self, target_date_str: str):
        self.target_date = target_date_str
        self.pipeline_name = self.__class__.__name__
        self._last_result: Any = None # 최종 Task 결과를 저장

    @property
    @abstractmethod
    def _steps(self) -> List[Task]:
        """
        [필수 구현] 파이프라인이 실행할 Task 객체들의 리스트를 반환해야 합니다.
        각 Task는 순서대로 실행되며, 이전 Task의 결과가 다음 Task의 입력으로 전달됩니다.
        """
        pass

    def run(self):
        """파이프라인에 정의된 모든 Task를 순차적으로 실행합니다."""
        print(f"\n================================================")
        print(f"🚀 Pipeline Start: {self.pipeline_name} ({self.target_date})")
        print(f"================================================")

        current_input = None
        
        try:
            for i, task in enumerate(self._steps):
                task_name = task.__class__.__name__
                print(f"[Step {i+1}/{len(self._steps)}] Executing Task: {task_name}")
                
                # 이전 Task의 결과를 현재 Task의 입력으로 전달
                result = task.execute(**current_input)
                
                # Task 결과 상태 확인 및 파이프라인 중단 로직
                if result.get('status') == 'FAILED':
                    print(f"❌ Pipeline Halted: Task {task_name} failed. Error: {result.get('error', 'No details')}")
                    self._last_result = result
                    return
                
                current_input = result
            
            self._last_result = current_input
            print(f"\n================================================")
            print(f"✅ Pipeline Complete: {self.pipeline_name}")
            print(f"================================================")

        except Exception as e:
            print(f"\n================================================")
            print(f"🔥 Critical Pipeline Failure: {e}")
            print(f"================================================")
            self._last_result = {'status': 'CRITICAL_ERROR', 'error': str(e)}

    def get_final_result(self) -> Any:
        """파이프라인 실행 후 최종 Task의 결과를 반환합니다."""
        return self._last_result
