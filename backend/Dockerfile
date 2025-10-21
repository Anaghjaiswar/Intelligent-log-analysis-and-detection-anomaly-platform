FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/usr/local/bin:${PATH}"

# Install dependencies
# Using a virtual environment is a good practice
COPY requirements.txt .
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

# Create non-root user
RUN adduser --disabled-password appuser
USER appuser

# Activate the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# Default command for the API container (gunicorn in docker-compose)
CMD ["gunicorn", "core.wsgi:application", "--bind", "0.0.0.0:8000"]
