# Frequenz Dispatch Client Library

[![Build Status](https://github.com/frequenz-floss/frequenz-client-dispatch-python/actions/workflows/ci.yaml/badge.svg)](https://github.com/frequenz-floss/frequenz-client-dispatch-python/actions/workflows/ci.yaml)
[![PyPI Version](https://img.shields.io/pypi/v/frequenz-client-dispatch)](https://pypi.org/project/frequenz-client-dispatch/)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen)](https://frequenz-floss.github.io/frequenz-client-dispatch-python/)

## 🚀 Introduction

Welcome to the **Frequenz Dispatch Client Library**—your go-to Python client for low-level interactions with the Frequenz Dispatch API!

If you're a developer who needs direct access to the [Dispatch API](https://github.com/frequenz-floss/frequenz-dispatch-api) without the abstraction layers of the high-level client, you're in the right place. This library serves as the foundation for our more feature-rich and user-friendly [high-level client](https://github.com/frequenz-floss/frequenz-dispatch-python).

## 📦 Installation

Install the library via pip:

```bash
pip install frequenz-client-dispatch
```

## 🛠️ Usage

Here's a quick example to get you started:

```python
from frequenz.client.dispatch import Client
import asyncio

async def print_dispatches():
    # Initialize the client
    client = Client(key="your_api_key", server_url="grpc://fz-0004.frequenz.io")

    # List all dispatches for a specific microgrid
    async for page in client.list(microgrid_id=1):
        for dispatch in page:
            print(dispatch)

# Run the Example
asyncio.run(print_dispatches())
```

For detailed usage and advanced features, check out the [client documentation](https://frequenz-floss.github.io/frequenz-client-dispatch-python/latest/reference/frequenz/client/dispatch/#frequenz.client.dispatch.Client).

## 🌐 Supported Platforms

We officially support and test the following platforms:

- **Python:** 3.11
- **Operating System:** Ubuntu Linux 20.04
- **Architectures:** amd64, arm64

## 🤝 Contributing

We welcome contributions! If you're interested in building or improving this project, please read our [Contributing Guide](CONTRIBUTING.md) to get started.
