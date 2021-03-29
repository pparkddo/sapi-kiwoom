# sapi-kiwoom
키움증권 OpenAPI 서버 프로그램입니다. 서버과 클라이언트의 통신은 메시지큐를 통해서 이루어집니다. `sapi-kiwoom` 서버를 실행함으로써 클라이언트 프로그램을 서버와 분리된 환경에서 실행할 수 있습니다.

## Architecture
![sapi-kiwoom architecture](https://gist.githubusercontent.com/pparkddo/bf7cdd18b92a64988a71acdaf01ea004/raw/0b0d36195764108d4cd81044b63a5ff9bbd8c024/sapi_kiwoom_concept.svg)

## Installation
```
git clone https://github.com/pparkddo/sapi-kiwoom.git
pip install -r sapi-kiwoom/requirements.txt
```

## Example
- 서버 실행
  ```
  cd sapi-kiwoom
  python -m sapi_kiwoom amqp://localhost:5672
  ```
- 클라이언트 실행
  ```python
  import json
  from datetime import datetime

  import pika

  # 요청할 내용을 json 으로 만듭니다.
  request = {
      "task_id": "custom-task-id",
      "method": "get-stock-name",
      "parameters": {"stock_code": "015760"},
      "request_time": datetime.now(),
  }
  request_body = json.dumps(request, ensure_ascii=False, default=str)
  
  # 메시지큐에 연결하고 요청내용을 tasks 큐에 전송합니다.
  # properties 를 추가하고 reply_queue 값에 큐 이름을 지정하면
  # 자동으로 서버에서 기본 응답큐가 아닌 reply_queue 에 있는 값으로 응답을 보냅니다.
  connection = pika.BlockingConnection(pika.URLParameters("amqp://localhost:5672"))
  channel = connection.channel()
  channel.basic_publish(
    exchange="",
    routing_key="tasks",  # default send queue : tasks
    body=request_body
  )

  # 메시지를 받기위해 기본 응답 큐를 consume 합니다.
  receive_messages = []
  channel.basic_consume(
      queue="sapi-kiwoom",  # default receive queue : sapi-kiwoom
      on_message_callback=lambda *args: receive_messages.append(args[3]),
      auto_ack=True
  )

  while not receive_messages:
      connection.process_data_events()

  # 응답으로 온 메시지를 json 으로 파싱해 확인합니다.
  response_body = json.loads(receive_messages[0])
  print(response_body)
  ```

## Message Format
- Request (JSON)
  ```
  {
    "task_id": "custom-task-id",
    "method": [sapi_kiwoom/kiwoom/method.py 참고],
    "parameters": [lookup.py, rt.py, task.py 참고],
    "request_time": "2021-03-28T12:53:41.820Z"
  }
  ```
- Response (JSON)
  ```
  {
    "task_id": "custom-task-id",
    "result": [result(1), ..., result(n)],
    "response_time": "2021-03-28T12:54:00.500Z",
    "status": "TASK_SUCCEED" | "TASK_FAILED",
  }
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