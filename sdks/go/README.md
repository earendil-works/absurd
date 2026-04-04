# Go SDK Layout

This directory intentionally contains multiple Go modules:

- `absurd/`: the published Go SDK module
- `tests/`: integration/parity test module for the Go SDK

Keeping the SDK in `sdks/go/absurd` ensures the package import name is:

```go
import "github.com/earendil-works/absurd/sdks/go/absurd"
```

instead of importing a package named `go`.
