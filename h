[33mcommit 16bc194ef588ed4a1fd90529393116b160e02720[m[33m ([m[1;36mHEAD[m[33m -> [m[1;32mmaster[m[33m)[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Mon Sep 21 00:41:15 2026 +0900

    Fix Notion database to data source resolution

[33mcommit 71d63bec948eb4002cfe5834a8e9442b5b9c913f[m[33m ([m[1;31morigin/master[m[33m)[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Tue Sep 15 16:01:15 2026 +0900

    feat: add NOL ticket crawler and fix region filtering

[33mcommit c2371516c2f1d169b10eaf175b15ace6b4b40c72[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 20:56:04 2026 +0900

    feat: 티켓 오픈 회차/기간 데이터 정규화 및 추출 로직 개선, 크롤러 전반 업데이트

[33mcommit 6165b7df1fac6b37298719d2fe019419fbf44300[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 20:17:10 2026 +0900

    feat: 공연 기간 및 티켓 오픈 기간 통합 처리 로직 추가, 크롤러 및 데이터 정규화 개선

[33mcommit e29d41f9f24a0376e8c6f180176fa7ed8a0d4016[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 18:45:50 2026 +0900

    feat: 공연 기간 추출 및 통합 처리 기능 추가, 관련 크롤링 로직 개선

[33mcommit a9004ee8e8806ba5e943d7770ec20254d51987b5[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 18:35:25 2026 +0900

    feat: 티켓 오픈 기간 추출 로직 추가 및 공연 기간 식별 개선

[33mcommit babef9180fe8d7d43461e7b02764b182f5648bf2[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 15:21:20 2026 +0900

    feat: 상세 섹션 크롤링 로직 개선 및 ticketDates 기반 일정 추출 추가

[33mcommit 0db5be5a658e5d08b5b9fec079705a8b618b2eac[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 14:41:46 2026 +0900

    feat: 공연 기간 추출 로직 추가 및 기존 로직 개선

[33mcommit a6e9ebbe4c17ffc2d6ec9886c43c76b0bdd0643c[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 14:27:05 2026 +0900

    fix: normalize_title 로직 수정 및 중복 처리 제거

[33mcommit 6b163133ce6b10d48547ee780d9ac9da43619db8[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 14:03:28 2026 +0900

    feat: 노션 변경 처리

[33mcommit 5e9a95356934f730be1fc7050bf63efe07159ec2[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 3 13:44:19 2026 +0900

    feat: 오류 수정 및 yes24 및 lg아센 추가

[33mcommit 8725438d59f3ba02601494aea726261be7d18f6e[m
Author: GitHub Actions <github-actions@github.com>
Date:   Wed May 6 18:19:24 2026 +0900

    fix: ticketlink 크롤링 로직 수정 및 CAST 헤더 처리 로직 개선

[33mcommit 19ed820a8bfdc2f9494d4effe68eb50bb3d6cd59[m
Author: GitHub Actions <github-actions@github.com>
Date:   Wed May 6 18:02:46 2026 +0900

    fix: Notion API 타임아웃 처리 로직 추가 및 디버깅용 로그 출력

[33mcommit 4968697f5f10c78adb8644a173eb5eb0bf604313[m
Author: GitHub Actions <github-actions@github.com>
Date:   Wed May 6 17:44:39 2026 +0900

    fix: 개선된 크롤링 안정성 및 데이터 처리 수정

[33mcommit 58c73ef8ab73cf6021443ed1a3cec12f0e8945ac[m
Author: GitHub Actions <github-actions@github.com>
Date:   Wed May 6 17:05:34 2026 +0900

    fix: reserveWebUrl 조건 오류 수정

[33mcommit eb18f9220ba8131dc1faf3db3de4b7b60a6c39c1[m
Author: GitHub Actions <github-actions@github.com>
Date:   Wed May 6 16:49:15 2026 +0900

    fix: 로깅 도입 및 크롤러 기능 안정화

[33mcommit c0f6c7237ff876de67b2e031426b42dd9f919f0d[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Sat Jan 31 00:06:27 2026 +0900

    Remove debug print for region conversion
    
    Remove debug print statement for region information.

[33mcommit 732d65d351a71e110c456c32f66b0b3146879071[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Sat Jan 31 00:04:54 2026 +0900

    Fix region print statement to handle missing values

[33mcommit ad5f640855cf2a2b2959bf6bdb76584200062866[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Jan 30 19:29:24 2026 +0900

    Fix print statement to show original region correctly

[33mcommit 72f399cb2bbaab1fa1e792b0c0ecc566d590edc0[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Jan 30 19:29:04 2026 +0900

    Fix typo in region logging statement

[33mcommit 5792809c8cea7e36cf94fb9f797cd8aa8b8c69e4[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Jan 30 19:25:07 2026 +0900

    Add debug print for venue and region extraction
    
    Add debug print statement for region conversion

[33mcommit fde21d184bd75ff1d9a22aafeaefb9be8f9cd887[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Jan 30 19:23:46 2026 +0900

    Log region conversion for venue information
    
    Add print statement for region conversion from venue

[33mcommit 59d2f3f8f9e69380f09efb72aeb5b78f84e76181[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Jan 30 19:19:44 2026 +0900

    Correct region print statement typo
    
    Fix typo in print statement for region information.

[33mcommit 1978e84617559654923bf8b863c6241ba649c7b6[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Jan 30 19:16:20 2026 +0900

    Fix date range to 7 days and enable all crawlers

[33mcommit 8587b5ca5247c52ecf537649705253db26d0cf26[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Jan 30 19:10:02 2026 +0900

    temp

[33mcommit 452d1abd2abc9ffa32eda013885ecb3068f3f6d2[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 14:21:24 2025 +0900

    fix:오류 수정

[33mcommit 048cba526e15dd204e9603f56f418955fd9b8141[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 14:10:36 2025 +0900

    fix:오류 수정

[33mcommit 1d60198c30fc8d6807e5bcbfb62fe5d6afbac532[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 14:05:11 2025 +0900

    fix:오류 수정

[33mcommit a3a557d5eadef529a745e6a670807eca7f2d3e72[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 13:58:15 2025 +0900

    feat: 타이틀 중복 제거

[33mcommit 01f5983b28d27a27dd0fe0aae0634a97d355e809[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 13:41:49 2025 +0900

    feat: 타이틀 중복 제거

[33mcommit 0a0a7c0d2dc3315a9dba012ae868e6bde571180b[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 13:17:58 2025 +0900

    feat: 타이틀 포함으로 변경

[33mcommit d7e856b07b6be3ce909cfc875efebf24a1a87f50[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 12:59:15 2025 +0900

    feat: 타이틀 정제

[33mcommit 519df431416f1bc155b9ee9715fb0bec82e1e236[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 12:55:51 2025 +0900

    feat: 타이틀 정제

[33mcommit b7243e34283049737a93878f7ee6a427abba102b[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 12:51:53 2025 +0900

    feat: 주요 공연 추가

[33mcommit 4d8385b836c946710a0b31fc1799f2cbbc91b43f[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 12:30:03 2025 +0900

    fix: notion api 변경에 따른 수정

[33mcommit 611663a286d31449928900090afd0783c39d9226[m
Author: GitHub Actions <github-actions@github.com>
Date:   Mon Oct 27 10:38:52 2025 +0900

    feat: 크롤링 원복

[33mcommit 70e3a61137a9fb476c0c40ee27dbec748f3ee6b7[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Oct 24 19:43:40 2025 +0900

    Remove title database references and logic
    
    Comment out unused title database and mapping logic.

[33mcommit d7d1d4018277ee2c74f3dcad87268f6ffa94b735[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Fri Oct 24 19:41:23 2025 +0900

    Update follow_run.py

[33mcommit c8a7e5330a4c617136329dd3ecf89be8b1f821b4[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Oct 24 18:44:54 2025 +0900

    feat: follow 공연 추가 원복

[33mcommit e61fc3b1d49b8769735f562b8155ffebdd7c9a9d[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Oct 24 16:33:13 2025 +0900

    feat: follow 공연 추가

[33mcommit 3eee87f2c8caf918b1dc24c8681cf183dc6fd436[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Oct 24 13:49:20 2025 +0900

    feat: follow 공연 추가

[33mcommit b7794fe1a8e340431b4934c64b7d818c30810547[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Oct 24 13:33:33 2025 +0900

    feat: follow 공연 추가

[33mcommit 08944ef4cbd03ec843388445379b789a24cc4396[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Oct 2 18:15:12 2025 +0900

    feat: 티켓링트 크롤링 추가

[33mcommit 8f783f02b0590a2b6c75e614052afd75422e4933[m
Merge: c3b2e74 10ee2ca
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Aug 21 15:17:14 2025 +0900

    Merge remote-tracking branch 'origin/master'

[33mcommit c3b2e7415b712ad098e7395ade35823ec2b77067[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Aug 21 15:17:02 2025 +0900

    fix: map region names to localized values in interpark ticket data

[33mcommit 10ee2cab149a20ef3c58bb0cf4f24b442df8e462[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Thu Aug 21 15:03:19 2025 +0900

    Update ReadMe.md

[33mcommit 006bfb426938c8920c5cc58ada10c5ca5c73ff0a[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Aug 21 15:01:43 2025 +0900

    fix: update region field to regions in interpark and melon ticket data

[33mcommit 4abe4665edc8492f74ce3ef19bfcd370c3790a05[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Aug 21 14:51:58 2025 +0900

    fix: add region information to ticket data in writer.py

[33mcommit 96a52bb2da9ec96594bd1a9d19e369f0a5a2e143[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Aug 21 14:51:20 2025 +0900

    fix: add region information to ticket data in writer.py

[33mcommit 1cdad89cf657b3edc1c64f25de4950e461079687[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Aug 21 14:33:29 2025 +0900

    fix: enable writing merged ticket data to Notion repository

[33mcommit 08be8cf270b22049881818ec31b2613c40d3983a[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Aug 21 14:22:24 2025 +0900

    fix: enhance inter_park and melon crawlers to include region information in ticket data

[33mcommit 4c62aaea2e3046ee50e0feaed2c61e7f6f6f3949[m
Merge: 9d342d9 279c9a7
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 4 11:07:11 2025 +0900

    Merge remote-tracking branch 'origin/master'

[33mcommit 9d342d985c62f89d2f7dfc164b7cf032b487fa4a[m
Author: GitHub Actions <github-actions@github.com>
Date:   Fri Jul 4 11:06:51 2025 +0900

    fix: expand regions in inter_park configuration to include BUSAN and ULSAN

[33mcommit 279c9a79b67ec80eb246de9c85a6932b69cdbc49[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 02:17:49 2025 +0900

    fix: update providers and source in interpark.py to reflect new ticketing service

[33mcommit 646c0b407c3c19866a5196d5f071d1e3f2b64344[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 02:05:00 2025 +0900

    fix: update event description formatting to separate providers and cast names

[33mcommit f5d631924635ec1696e15f99216f2bd048adcd2a[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 01:59:52 2025 +0900

    fix: refactor event description to include unique cast names and improve detail link handling

[33mcommit 7c68a5d7dd81e2f2385fd956ed6058971e47fbed[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 01:54:08 2025 +0900

    fix: update event description to include unique cast names from title

[33mcommit 24614927d0143a2fbe9ed310e4446496fb46a781[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 01:45:30 2025 +0900

    fix: update event description formatting to separate providers and cast names

[33mcommit b056698b373e5431ea90ad4748042ca275b1ed94[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 01:40:00 2025 +0900

    fix: update cron schedule to run weekly on Sunday and Wednesday

[33mcommit 3e6bdfcde77ee031fbd39363c2a4b3729b69c25f[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 01:36:59 2025 +0900

    fix: update event description to include cast names and clean up unused file link code

[33mcommit 8866e5935965104b8504d11154f8debcd76b8c20[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 01:02:31 2025 +0900

    feat: enhance _build_contents to include external file link for ICS URL

[33mcommit a9590d8e17129bacf7f78a1d06967f03999b4f2f[m[33m ([m[1;33mtag: [m[1;33mv1.0[m[33m)[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 00:37:26 2025 +0900

    fix: add write permissions for contents in ticket_schedule.yml

[33mcommit 319502a456b9c4cdea992e711c3c4556ac7c8c7e[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 00:30:20 2025 +0900

    fix: update ticket_schedule.yml to improve .ics file handling and cleanup process

[33mcommit eb16881e56786225ac4aca3d186e4427d05208bb[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 00:19:07 2025 +0900

    fix: remove checkout action from ticket_schedule.yml

[33mcommit ae83acacc6efc1dfaf045b7f5866bbd031608978[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 00:15:42 2025 +0900

    fix: set default values for GB_ICAL_DIR and GB_BRANCH in config.py; update paths in ticket_schedule.yml

[33mcommit a54306273fcd0ed13259a8a2a46f0e34247ff765[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Fri Jun 6 00:06:33 2025 +0900

    feat: add logging for branch and directory in ticket_schedule.yml

[33mcommit 386266f4dfe11b18559cd150287bc323727c6c18[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Thu Jun 5 23:56:13 2025 +0900

    fix: remove duplicate checkout action in ticket_schedule.yml

[33mcommit c40b6028302b98972350f1a2b654a2c1103828fc[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Thu Jun 5 23:49:28 2025 +0900

    init

[33mcommit e3a4488ee5e3d2fb80db0025d86f4c477298117d[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Thu Jun 5 23:35:26 2025 +0900

    Update .gitignore

[33mcommit 64b152d2d4c1b8776df12d309c53572cf090a240[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Thu Jun 5 23:26:07 2025 +0900

    Delete .env

[33mcommit 622cffe63ddd2662b9db1c7cb34ed272dfe0300b[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Jun 5 21:50:50 2025 +0900

    fix: update ReadMe.md and ticket_schedule.yml for correct environment variable usage

[33mcommit 85c06b89dd98a457de157cb97c07533a0d978e53[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Jun 5 21:48:09 2025 +0900

    add .gitignore and update configuration for GitHub Actions integration

[33mcommit 53d449a2e9b74836c0e6fcf48c49dc2416c9febb[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Jun 5 21:45:14 2025 +0900

    add .gitignore and update configuration for GitHub Actions integration

[33mcommit f1feb338a09a522a55dddb53e61698a351b59d52[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Jun 5 21:44:58 2025 +0900

    add .gitignore and update configuration for GitHub Actions integration

[33mcommit 3c9e962f9f06a688d264cf410bd3a2f4c707de6c[m
Author: GitHub Actions <github-actions@github.com>
Date:   Thu Jun 5 21:41:33 2025 +0900

    add .gitignore and update configuration for GitHub Actions integration

[33mcommit b70cf69430a36f2c0b4d1c0e69152ac44382c855[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 20:04:53 2025 +0900

    git username change

[33mcommit da7b903e88ad79f7ab54900df8b02ec62771a7e9[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 20:04:35 2025 +0900

    git username change

[33mcommit 92fc36c2d24799dba2e0c9e506ff7460bb3992e7[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 19:57:50 2025 +0900

    git username change

[33mcommit 261a5ae9c9f706a204c5887ec9540589bb228e89[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 19:52:24 2025 +0900

    git setting change and log remove

[33mcommit f824b50816e2565d71f75dfc330f3bb49d9cd437[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 19:49:31 2025 +0900

    git setting change and log remove

[33mcommit 7c5fe286bffa458e6d8cdf5b9a476aa4640d15b6[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 19:32:34 2025 +0900

    git user setting

[33mcommit 3a7e7de4e1b6ce02a113ed0bba607807ee86ce09[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 19:27:05 2025 +0900

    config change

[33mcommit d9fa1dd209a8c5bcfba0dcdcd9e205c68f27ac7b[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 19:18:13 2025 +0900

    config change

[33mcommit 37a52c90ba0b90aa7df24320878c9f54b60a6c23[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 19:09:10 2025 +0900

    config change

[33mcommit a9bd870e031698e2fcc59fe6f0ca6db41b9ce148[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 18:49:18 2025 +0900

    config change

[33mcommit 6df4c089b122af69ef094b7ea4b96e6bb2458b16[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 18:43:19 2025 +0900

    config change

[33mcommit 1f12072fa984884a603941a3bf5bfc09df1a648d[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 18:38:53 2025 +0900

    env add

[33mcommit 64a194ff4a3c2b36421f4201f00a272827e90417[m
Author: minkyungHam <lmkhaml@gmail.com>
Date:   Thu Jun 5 18:16:15 2025 +0900

    등록 링크 추가

[33mcommit 0c78ddd55b8df3d21273834de558d021c128ff28[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Wed Jun 4 15:27:52 2025 +0900

    포함 인원 -> 제목에서도!

[33mcommit 24e42364c645ae3d38294710bb66590c8d9742df[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Mon May 26 16:12:12 2025 +0900

    사이트 크롤링 실패해도 실행 되도록 수정

[33mcommit 01a77845c3530bac0878854cd1033ace5192d08f[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Mon May 26 15:15:53 2025 +0900

    사이트 크롤링 실패해도 실행 되도록 수정

[33mcommit 391afa413a11c69f1c4bb9c75ae5250875cdacab[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Wed May 21 20:26:35 2025 +0900

    주요 배우 설정 추가

[33mcommit cbfb91a56cf9cc9ed1b8f35d04cb7f0af33aa9d1[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Wed May 21 20:25:05 2025 +0900

    주요 배우 설정 추가

[33mcommit 1d6ea932e4433870574499796f65cd091258c872[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Wed May 21 18:04:51 2025 +0900

    오픈 회차 추가 및 오류 수정

[33mcommit 5efc651dbfd0d103b0136b864b252583b1e047c4[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Wed May 21 17:18:12 2025 +0900

    timeZone & 예술의 전당추가

[33mcommit 4b1d6558d224839d278f524d9ee280ecca22eeab[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Wed May 21 17:18:07 2025 +0900

    timeZone & 예술의 전당추가

[33mcommit 6808c51f7d07bc1ab0d9038df918f7826263a608[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Wed May 21 17:17:48 2025 +0900

    timeZone & 예술의 전당추가

[33mcommit 1898342c7075c8a33bfd3ce3df0d4b84b9dff169[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Tue May 20 15:00:26 2025 +0900

    오픈타입 여러개인 경우 처리

[33mcommit 3db4ffac808fec21ebeeb74da4c255cfcab76d32[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Tue May 20 14:56:16 2025 +0900

    세종 추가 및 에러 수정

[33mcommit 49a0d60d40708d2651a0c0480b7776e6421112a6[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Tue May 20 11:17:28 2025 +0900

    스케쥴링 매일, 당일 + 7일

[33mcommit b6e73f2f2a53becaf1cc10e3ff63fc1bf16b69dc[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Mon May 12 18:24:18 2025 +0900

    세종 추가 및 오류 수정

[33mcommit 6a4c9920eae514965b1abc015730232679b7124b[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Mon May 12 11:02:24 2025 +0900

    run.py 업데이트

[33mcommit f911b7ae30890aa796b1c7b6f5ef7be197d4c4f7[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Mon May 12 10:53:17 2025 +0900

    run.py 업데이트

[33mcommit d007901516eeeef9d795e9b56cffd291e14c479e[m
Merge: 1eba5f9 c963108
Author: minkyungHam <mk.ham@innods.com>
Date:   Thu May 8 17:51:18 2025 +0900

    Merge remote-tracking branch 'origin/master'

[33mcommit 1eba5f90e855e6d64d94618b6b6340e46306a811[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Thu May 8 17:50:31 2025 +0900

    11

[33mcommit e49772ee9e7cf0e5415439644abedee5d6889f11[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Thu May 8 17:40:29 2025 +0900

    11

[33mcommit c9631083036aaa6836db9447b1fbbebd204cb75e[m
Author: hamjjang <61483029+HamMinKyung@users.noreply.github.com>
Date:   Wed May 7 21:10:58 2025 +0900

    Update run.py
    
    11

[33mcommit dea041e6c5bb1dd87f81a8d0327698355224d7fa[m
Author: minkyungHam <mk.ham@innods.com>
Date:   Wed May 7 21:09:20 2025 +0900

    11

[33mcommit 197277b6fcee319fe2661dd968e5486baa58382c[m
Author: hamjjang <lmkhaml@gmail.com>
Date:   Wed May 7 12:39:00 2025 +0900

    init
