# EVE Killmap Backend

The public HTTP and WebSocket API for [EVE Killmap](https://eve-killmap.com). It serves killmail, sovereignty, statistics, and other data to the [frontend](https://github.com/eve-killmap/frontend) from the shared kills database, which is kept current by [process-kills](https://github.com/eve-killmap/process-kills). It also broadcasts live kills to the frontend via WebSocket. It runs as several uvicorn workers in a leader/follower fashion.

Requires Python 3.12+, PostgreSQL, and Redis. Install `requirements.txt`, copy `.env.example` to `.env` and fill in `DATABASE_URL` and the other values, optionally copy `config.example.yml` to `config.yml`, then run `uvicorn app.main:app`. The test suite (`requirements-dev.txt`, `python -m pytest`) needs no network, credentials, or database.

## Bugs and feature requests

Please report bugs and request features in the [frontend repository](https://github.com/eve-killmap/frontend/issues), which is the main entry point for the project. Open an issue here only if you are sure the problem originates in this API.
