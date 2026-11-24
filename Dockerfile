FROM starwitorg/base-python-image:0.0.15 AS build

# Copy only files that are necessary to install dependencies
COPY poetry.lock poetry.toml pyproject.toml /code/

WORKDIR /code
# Install runtime dependencies only (no dev group) and strip pip from the venv
RUN poetry install --only main --no-root && \
    rm -rf .venv/lib/python3.12/site-packages/pip* .venv/bin/pip*


### Main artifact / deliverable image

FROM python:3.12-slim

# Create a non-root user and group
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /code
COPY --from=build --chown=appuser:appgroup /code/.venv /code/.venv
COPY --chown=appuser:appgroup main.py /code/
COPY --chown=appuser:appgroup rediswriter /code/rediswriter

# Switch to non-root user
USER appuser

ENV PATH="/code/.venv/bin:$PATH"
CMD [ "python", "main.py" ]
