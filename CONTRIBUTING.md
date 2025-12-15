# Contributing to go-rabbit

Thank you for your interest in contributing to `go-rabbit`!

## Getting Started

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally:
    ```bash
    git clone https://github.com/your-username/go-rabbit.git
    cd go-rabbit
    ```
3.  **Create a branch** for your feature or bug fix:
    ```bash
    git checkout -b feature/my-new-feature
    ```

## Development

### Dependencies

Ensure you have Go 1.22 or later installed.
Run `go mod download` to install dependencies.

### Testing

Run the tests to ensure everything is working:
```bash
go test -v ./...
```

### Linting

We use `golangci-lint`. Please ensure your code passes the linter before submitting.
```bash
golangci-lint run
```

## Submitting a Pull Request

1.  **Commit your changes** with clear, descriptive messages.
2.  **Push to your fork**:
    ```bash
    git push origin feature/my-new-feature
    ```
3.  **Open a Pull Request** on the main repository.
4.  Ensure the CI checks pass.

## Code Style

- Follow standard Go conventions (Effective Go).
- Document exported functions and types.
- Format your code with `gofmt` (or `goimports`).

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
