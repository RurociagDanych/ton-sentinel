FROM spark@sha256:60b99fa37465eac7749f2bad2ab90eb4b52dc04584a8b1b163f10926c82a7045
USER root
# Dependency installation only inside image, not host/global Python.
COPY requirements.lock /opt/probe/requirements.lock
RUN python3 -m pip install --no-cache-dir --require-hashes --target /opt/probe/deps -r /opt/probe/requirements.lock
COPY jars/ /opt/probe/jars/
RUN python3 -c "from pathlib import Path; [p.unlink() for pattern in ('lz4-java-*.jar','checker-qual-*.jar') for p in Path('/opt/spark/jars').glob(pattern)]" \
    && cp /opt/probe/jars/*.jar /opt/spark/jars/ && useradd --uid 1000 --create-home sentinel
# Package source incl. packaged resources (demo.html, dashboard/*) so the image runs without a src bind mount.
# Build context must hold requirements.lock, jars/ and src/ton_sentinel (root-owned, read-only to uid 1000).
COPY src/ton_sentinel/ /workspace/src/ton_sentinel/
ENV PYTHONPATH=/opt/probe/deps:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/workspace/src:/workspace
USER 1000:1000
WORKDIR /workspace
