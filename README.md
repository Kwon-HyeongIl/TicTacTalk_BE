# TicTacTalk
이미지

<br>

## 🚀 프로젝트 소개

TicTacTalk은 대화나 상황 속에서 발생한 갈등이나 문제 상황을 객관적으로 분석하여,
참여자 각자의 책임도와 잘못의 정도를 정량적으로 평가하는 AI 기반 분석 플랫폼입니다.

사용자의 대화 내용, 감정 변화 등을 종합적으로 분석해 누가 얼마나 잘못했는지 데이터를 기반으로 시각화해줍니다.

<br>

## 👥 팀원 소개
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/itwillbeoptimal.png" width="80"><br>
      <a href="https://github.com/itwillbeoptimal"><b>김지훈</b></a>
    </td>
    <td align="center">
      <img src="https://github.com/Kwon-HyeongIl.png" width="80"><br>
      <a href="https://github.com/Kwon-HyeongIl"><b>권형일</b></a>
    </td>
    <td align="center">
      <img src="https://github.com/kokeunho.png" width="80"><br>
      <a href="https://github.com/kokeunho"><b>고근호</b></a>
    </td>
    <td align="center">
      <img src="https://github.com/leesy010504.png" width="80"><br>
      <a href="https://github.com/leesy010504"><b>이상윤</b></a>
    </td>
  </tr>
  <tr>
    <td align="center">FE · 팀장</td>
    <td align="center" colspan="3">BE</td>
  </tr>
</table>

<br>

## ⚙️ 기술적인 부분 (백엔드)

- **MSA 아키텍처**: 프로젝트를 Apigateway-Service, Security-Service, Clova-Service, Rag-Service, Chat-Service의 마이크로 서비스로 설계해서 기능별로 독립 배포가 가능하도록 하였습니다.

- **Clova API 기반 음성 데이터 변환**: 사용자의 음성 대화 데이터를 Clova API를 통해 이후에 처리할 감정 평가에 용이하도록 규격화된 텍스트 데이터로 변환시킵니다.

- **RAG 검색 서비스**: 감정 평가 전 대화 라벨링을 위해 PostgreSQL 기반 트라이그램 Top-K 유사도 검색으로 맥락이 유사한 데이터셋 대화 문장을 찾아 라벨 부여 정확도를 높였습니다.

- **웹소켓 기반 채팅 서비스**: 웹소켓으로 실시간 채팅을 제공하고 생성된 대화 로그를 감정 평가용 데이터로 수집 및 활용하도록 설계했습니다.

