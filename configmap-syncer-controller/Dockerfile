# Use a slim Python image for a smaller footprint

FROM python:3.11-slim
# Set the working directory in the container
WORKDIR /app

# Install dependencies

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the controller script into the container

# NOTE: Ensure your controller script is named 'cm_syncer.py'

COPY cm_syncer.py .

# Command to run the controller when the container starts

CMD ["python", "cm_syncer.py"]
