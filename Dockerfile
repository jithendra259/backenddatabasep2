# Use a lightweight Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application code into the container
COPY . .

# Expose any necessary port (optional, if you have an API endpoint)
EXPOSE 8080

# Set the command to run your app.
# If your script is designed to run continuously (like a background worker), use:
CMD ["python", "app.py"]
