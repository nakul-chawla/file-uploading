FROM python:3.9-slim as venv

# Install C++ Compiler
RUN apt update && apt -y install g++

# Build and install python requirements
COPY ./requirements.txt /tmp/
RUN pip install wheel && pip install -r /tmp/requirements.txt

# Install runtime dependencies
RUN apt -y install --no-install-recommends libmagickwand-dev ghostscript inotify-tools

# Needed for writing locally
RUN mkdir /scratch

# Copy app source
COPY ./src /app

# Remaining setup
COPY start.sh start.sh
RUN chmod +x /start.sh

WORKDIR /app
ENV PYTHONPATH=/
EXPOSE 80
CMD ["/start.sh"]
