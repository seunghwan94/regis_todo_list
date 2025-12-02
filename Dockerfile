FROM python:3.10-slim

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create attachments directory at runtime
RUN mkdir -p /app/attachments

# Expose port
EXPOSE 8000

# Start the application
CMD ["python", "main.py"]