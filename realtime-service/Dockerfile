FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app app/

# Expose port
EXPOSE 8083

# Run
CMD ["python", "-m", "app.main"]
