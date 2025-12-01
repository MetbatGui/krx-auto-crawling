# Use the requested uv base image with Python 3.14
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# Setup a non-root user
RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot

WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Install Playwright browsers and system dependencies
# This is required because the base image is 'slim' and lacks browser libs
# We run this as root before switching users
# Note: --with-deps installs system dependencies (requires root)
RUN playwright install-deps \
    && playwright install chromium

# Then, add the rest of the project source code and install it
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Change ownership of the app directory
RUN chown -R nonroot:nonroot /app

# Switch to non-root user
USER nonroot

# Entrypoint
ENTRYPOINT ["netbuy"]
CMD ["--help"]
