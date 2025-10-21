from abc import ABC, abstractmethod
from typing import Any

class Crawler(ABC):
    """모든 크롤러가 상속받아야 하는 추상 기본 클래스."""
    @abstractmethod
    def crawl(self, **kwargs) -> Any:
        """
        크롤링을 실행하고 가공된 데이터를 DataFrame으로 반환합니다.
        필요한 모든 인자는 이 메서드를 통해 받습니다.
        """
        pass
    
    def get_info(self) -> str:
        """크롤러의 정보를 반환합니다."""
        return f"Crawler: {self.__class__.__name__}"