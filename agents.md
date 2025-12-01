# AI Agent Guidelines

This document outlines the conventions and guidelines for AI agents working on the `krx-auto-crawling` project.

## Commit Message Convention

All commits must follow the format below to ensure a clean and readable history.

### Format

```
<gitmoji> <type>(<scope>): <subject>

<body>
```

*   **gitmoji**: An emoji representing the type of change.
*   **type**: The type of change (feat, fix, refactor, etc.).
*   **scope**: (Optional) The module or component affected (e.g., `main`, `adapter`, `service`).
*   **subject**: A concise description of the change in Korean.
*   **body**: A detailed explanation of the changes, bullet points are encouraged.

### Gitmoji & Type List

| Gitmoji | Type | Description |
| :--- | :--- | :--- |
| ✨ `:sparkles:` | `feat` | New feature implementation |
| 🐛 `:bug:` | `fix` | Bug fix |
| ♻️ `:recycle:` | `refactor` | Code refactoring without logic change |
| 🔧 `:wrench:` | `chore` | Configuration, build, or tooling changes |
| 📝 `:memo:` | `docs` | Documentation updates |
| ✅ `:white_check_mark:` | `test` | Adding or updating tests |
| 💄 `:lipstick:` | `style` | Code style changes (formatting, etc.) |
| 🚑 `:ambulance:` | `hotfix` | Critical hotfix |
| 👷 `:construction_worker:` | `ci` | CI/CD system or script changes |

## Release Process

To release the current `main` branch to the employer's repository (`employers-netbuy`), use the `just release` command.

```bash
just release
```

This command automates the following steps:
1.  Checkout or create a local `release` branch.
2.  Merge `main` into `release`.
3.  Push `release` to `employers-netbuy:main`.
4.  Switch back to `main`.

### Example

```
✨ feat(main): Google Drive 연동 및 Fallback 로직 적용

- GoogleDriveAdapter 초기화 로직 추가
- OAuth 2.0 인증 흐름 구현 (client_secret.json 사용)
- FallbackStorageAdapter를 통한 랭킹 리포트 로딩 안정성 확보
```

## Language Guidelines

*   **Language**: Always **think and speak in Korean**.
*   **Artifacts**: All artifacts (plans, reports, docs) must be written in **Korean**.

