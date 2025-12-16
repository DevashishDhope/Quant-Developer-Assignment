# ChatGPT Usage Transparency Note

## Overview
This document outlines the usage of AI tools (ChatGPT) during the development of the **Real-Time Quant Analytics Dashboard**.
The AI was primarily used as a **productivity tool** for syntax reference, library exploration, and troubleshooting specific error messages. 

## Usage Breakdown

### 1. Research & Library Selection
- **Purpose**: To quickly compare available Python libraries for WebSocket connections and interactive dashboarding.
- **Outcome**: Confirmed selection of `websockets` for async ingestion and `Streamlit` for rapid frontend prototyping.

### 2. Syntax & Boilerplate
- **Purpose**: To generate standard boilerplate code for Plotly charts and SQLite connection strings, saving development time.
- **Example Prompt**: *"How to update a Plotly figure in Streamlit using a placeholder?"*

### 3. Debugging Support
- **Purpose**: To interpret cryptic error messages (e.g., `asyncio` event loop errors) and suggest potential fixes.
- **Outcome**: Resolved multithreading conflicts between Streamlit's runner and the background ingestion thread.

## Summary
AI integration was limited to **accelerating implementation details**. All financial logic (Hedge Ratio, Z-Score, ADF Test) and system design choices were manually implemented and verified to ensure accuracy and performance.us on the high-level logic and financial concepts (Pairs Trading strategy).
