# sapi-kiwoom
키움증권 OpenAPI 서버 프로그램입니다. 서버과 클라이언트의 통신은 메시지큐를 통해서 이루어집니다. `sapi-kiwoom` 서버를 실행함으로써 클라이언트 프로그램의 실행환경 제약이 없어지고 클라이언트 프로그램에서 복잡한 GUI 이벤트 처리를 하지 않아도 됩니다. 

## Architecture
![sapi-kiwoom architecture](https://gist.githubusercontent.com/pparkddo/bf7cdd18b92a64988a71acdaf01ea004/raw/0b0d36195764108d4cd81044b63a5ff9bbd8c024/sapi_kiwoom_concept.svg)

## Installation
```
git clone https://github.com/pparkddo/sapi-kiwoom.git
pip install -r sapi-kiwoom/requirements.txt
```

## Example
```
cd sapi-kiwoom
python -m sapi_kiwoom amqp://localhost:5672
```

## Requirements
- `Python 3.8 (32bit)` : 키움 OpenAPI 는 32bit Python 에서만 실행 가능합니다.
- `RabbitMQ` : API 서버를 구성하기 위해 메시지큐로 RabbitMQ 를 사용합니다. Docker 로 실행하거나 실행파일을 [공식홈페이지](https://www.rabbitmq.com/download.html)를 통하여 설치하여 RabbitMQ 서버를 실행시켜야 합니다.
- `키움 OpenAPI` : 키움증권 홈페이지에서 키움 OpenAPI 를 설치해야합니다. API 사용을 위해서는 OpenAPI 사용신청도 해야합니다.
- `Windows OS` : 키움 OpenAPI 는 ActiveX 기반으로 Windows OS 에서만 동작합니다. Windows Server OS 도 공식으로 지원하지 않으니 정상적으로 실행이 되지않을 수 있습니다.
- `관리자 권한으로 실행` : 키움 OpenAPI 프로그램을 실행할 프로그램은 반드시 관리자 권한으로 실행되어야 합니다.

## Setup
- 32bit Python 설치

  ```
  :: Anaconda 에서 32bit 가상환경 설치
  set CONDA_FORCE_32BIT=1
  conda create -n py38_32bit python=3.8 python

  conda activate py38_32bit
  ```
- RabbitMQ 설치
  ```
  :: Docker 로 RabbitMQ 실행
  docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
  ```
- 키움 OpenAPI 설치
  - https://www3.kiwoom.com/nkw.templateFrameSet.do?m=m1408000000