---
trigger: always_on
---

# ANTIGRAVITY AGENT RULES (GENERIC)

## 1. The "Manager View" Workflow
* **Planning First:** For any task involving more than one file, you must first generate an **Implementation Plan Artifact**. Do not proceed to code until the user approves this plan.
* **Artifact Updates:** If the user comments on an artifact (Task List or Plan), treat those comments as high-priority instruction overrides. Update the artifact immediately.
* **Mode Selection:** * Use **Planning Mode** (Deep reasoning) for architectural decisions, refactoring, and complex logic.
    * Use **Fast Mode** only for single-file syntax fixes or small UI tweaks.

## 2. Verification & "The Trust Gap"
* **Evidence over Assurance:** Never just say "I fixed it." You must provide an **Artifact** to prove it.
    * **Visual Changes:** Generate a **Screenshot** or **Browser Recording** showing the new UI state.
    * **Logic Changes:** Generate a **Test Result Artifact** showing passing tests.
* **Browser Agent:** When building web interfaces, automatically use the Browser Agent to visit the localhost URL, interact with the new feature, and record the session.

## 3. Terminal & Security Policy
* **Execution Safety:** Assume the user is in **"Agent-Assisted"** mode (not Turbo). You must ask for permission before running distinct types of commands (e.g., database migrations, package installations).
* **Environment Integrity:** * Never hardcode secrets. Check for a `.env` file or ask the user to create one.
    * Before installing new packages (npm/pip), check the existing `package.json` or `requirements.txt` to avoid version conflicts.

## 4. Coding Standards & Editor Hygiene
* **Self-Documentation:** * Add docstrings/comments to all exported functions explaining *why* the logic exists.
    * Update the `README.md` with "How to Run" instructions if you add a new script or build step.
* **File Consistency:** Respect the existing project structure. Do not create new top-level folders without asking the Manager first.

## 5. Artifact Output Format
* **Diffs:** When showing code changes, always provide a standard **Code Diff Artifact** so the user can see context.
* **Walkthroughs:** At the end of a milestone, generate a **Walkthrough Artifact** summarizing:
    1.  What was changed.
    2.  How to verify it manually.
    3.  Known limitations or next steps.