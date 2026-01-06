#!/bin/bash
source venv/bin/activate

# Configure Gunicorn with better timeout and mobile-safe behavior
exec venv/bin/gunicorn app:app \
  --bind 0.0.0.0:7000 \
  --workers 2 \
  --timeout 60 \
  --keep-alive 5 \
  --log-level info \
  --access-logfile logs/gunicorn_access.log \
  --error-logfile logs/gunicorn_error.log
