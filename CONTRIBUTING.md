# Contributing to PerpLab

PerpLab is a Windows-first trading research platform. Changes should preserve the
connection between a strategy, its data, its execution assumptions, and its results.

## Set up

Follow the [README](README.md#quick-start), then install the test dependencies:

```powershell
.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

For frontend work, run `npm run dev` from `frontend/` alongside
`.venv\Scripts\python.exe -m perplab serve` from the repository root.
Vite forwards API requests to the backend at `127.0.0.1:8756`.

## Verify a change

```powershell
.venv\Scripts\python.exe -m pytest -q
npm --prefix frontend run build
```

The full backend suite runs on Windows, including native memory enforcement and
worker subprocess tests. Golden tests check hand-calculated financial scenarios;
integration tests compare replay results across separate processes.

## What to include in a pull request

- Explain the user-visible change and the problem it solves.
- Include the checks you ran and any limitations they leave unresolved.
- For accounting or execution changes, include a regression case with independently
  calculated expectations.
- Update the relevant documentation when defaults, data requirements, or commands change.

Keep changes focused. API responses and their TypeScript definitions must agree.
Execution changes must preserve event ordering and the reproducibility record.
Never commit `userdata/`, exchange credentials, generated builds, or local environment files.

## Report a bug

Open an [issue](https://github.com/anshul-vijay-chinchole/Perplab/issues) with the
steps to reproduce it, expected and observed behavior, Windows and Python versions,
and relevant logs. Remove account identifiers and credentials from attachments.
