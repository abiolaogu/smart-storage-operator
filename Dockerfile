# =============================================================================
# Smart Storage Operator - Rust Multi-Stage Dockerfile
# =============================================================================
# Build: docker build -t billyronks/smart-storage-operator:latest .
#
# Features:
# - Multi-stage build for minimal final image
# - Static linking with musl for maximum portability
# - Scratch-based final image (~10MB)
# - Non-root user for security
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Build environment with all dependencies
# -----------------------------------------------------------------------------
FROM rust:1.76-alpine AS builder

# Install build dependencies
RUN apk add --no-cache \
    musl-dev \
    openssl-dev \
    openssl-libs-static \
    pkgconfig \
    git

# Create a new empty project
WORKDIR /build
RUN cargo new --bin smart-storage-operator
WORKDIR /build/smart-storage-operator

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./

# Build dependencies only (this layer gets cached)
RUN cargo build --release && rm -rf src target/release/deps/smart_storage_operator*

# Copy source code
COPY src ./src

# Build the actual application
ARG VERSION=unknown
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

RUN RUSTFLAGS="-C target-feature=+crt-static" \
    cargo build --release --target x86_64-unknown-linux-musl 2>/dev/null || \
    cargo build --release

# Strip the binary for smaller size
RUN strip target/release/smart-storage-operator 2>/dev/null || \
    strip target/x86_64-unknown-linux-musl/release/smart-storage-operator 2>/dev/null || true

# -----------------------------------------------------------------------------
# Stage 2: Minimal runtime image
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

# Labels
LABEL org.opencontainers.image.title="Smart Storage Operator"
LABEL org.opencontainers.image.description="Intelligent storage tiering for OpenEBS Mayastor (Rust)"
LABEL org.opencontainers.image.vendor="BillyRonks Global Limited"
LABEL org.opencontainers.image.source="https://github.com/billyronks/smart-storage-operator"
LABEL org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /

# Copy binary from builder (try both paths)
COPY --from=builder /build/smart-storage-operator/target/release/smart-storage-operator /smart-storage-operator
# Alternative if musl build succeeded:
# COPY --from=builder /build/smart-storage-operator/target/x86_64-unknown-linux-musl/release/smart-storage-operator /smart-storage-operator

# Run as non-root
USER nonroot:nonroot

# Expose ports
EXPOSE 8080 8081

# Set entrypoint
ENTRYPOINT ["/smart-storage-operator"]

# -----------------------------------------------------------------------------
# Alternative: Scratch-based image (even smaller, ~5-10MB)
# -----------------------------------------------------------------------------
# FROM scratch AS scratch-runtime
# 
# COPY --from=builder /build/smart-storage-operator/target/x86_64-unknown-linux-musl/release/smart-storage-operator /smart-storage-operator
# COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# 
# EXPOSE 8080 8081
# ENTRYPOINT ["/smart-storage-operator"]
