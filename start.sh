#!/bin/bash
cd /app/frontend && npm run build
cd /app/backend && cargo run --release
