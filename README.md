# Base Microservice

This is a template for a basic microservice.

Feel free to add to and edit as required.

---
## What's included
- FastAPI (examples in `src/app/main.py`)
- Celery (examples in `src/app/workers/tasks.py`)
- SQLAlchemy (models in `src/app/models.py`)
- Alembic (migrations in `src/app/migrations/versions`)
- PostgreSQL
- RabbitMQ
- autoreload on code changes (works on most architectures)
- Imagemagick with PDF support (Ghostscript)
- [Wand library](https://docs.wand-py.org/) for Imagemagick

## Getting started

0) Install Docker
1) Clone the repository
2) Use `docker-compose up` in your Terminal to start the Docker container.
3) The app is defaulted to run on `localhost:8000`
   * `/`: The root url (contents from `src/main.py`)
   * `/health`: URL endpoint for a basic healthcheck. Displays alembic version and Celery worker ping responses. <br> Example of healthy response:
    ```json
    {
      "alembic_version":"c4f1de9fd1e1",
      "celery_response":[{"celery@fada80fe0ab9":{"ok":"pong"}}]
    }
    ```
   * `/test-task`: Runs a basic async Celery task.

## Migrating database
- `docker-compose stop`
- `docker-compose up`

## Rebuild infrastructure (not for code changes)
- `docker-compose build`

## Troubleshooting
- `docker-compose down` (This will destroy all your containers for the project)
- `docker-compose up`
