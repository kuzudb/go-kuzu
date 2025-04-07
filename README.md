# go-kuzu
[![Go Reference](https://pkg.go.dev/badge/github.com/kuzudb/go-kuzu.svg)](https://pkg.go.dev/github.com/kuzudb/go-kuzu)
[![CI](https://github.com/kuzudb/go-kuzu/actions/workflows/go.yml/badge.svg)](https://github.com/kuzudb/go-kuzu/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/kuzudb/go-kuzu)](https://goreportcard.com/report/github.com/kuzudb/go-kuzu)
[![License](https://img.shields.io/github/license/kuzudb/go-kuzu)](LICENSE)

Official Go language binding for [Kùzu](https://github.com/kuzudb/kuzu). Kùzu an embeddable property graph database management system built for query speed and scalability. For more information, please visit the [Kùzu GitHub repository](https://github.com/kuzudb/kuzu) or the [Kùzu website](https://kuzudb.com).

## Installation

```bash
go get github.com/kuzudb/go-kuzu
```

## Get started
An example project is available in the [example](example) directory.

To run the example project, you can use the following command:

```bash
cd example
go run main.go
```

## Docs
The full documentation is available at [pkg.go.dev](https://pkg.go.dev/github.com/kuzudb/go-kuzu).

## Tests
To run the tests, you can use the following command:

```bash
go test -v
```

## Windows Support
For Cgo to properly work on Windows, MSYS2 with `UCRT64` environment is required. You can follow the instructions below to set it up:
1. Install MSYS2 from [here](https://www.msys2.org/).
2. Install Microsoft Visual C++ 2015-2022 Redistributable (x64) from [here](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170).
3. Install the required packages by running the following command in the MSYS2 terminal:
   ```bash
   pacman -S mingw-w64-ucrt-x86_64-go mingw-w64-ucrt-x86_64-gcc
   ```
4. Add the path to `kuzu_shared.dll` to your `PATH` environment variable. You can do this by running the following command in the MSYS2 terminal:
   ```bash
   export PATH="$(pwd)/lib/dynamic/windows:$PATH"
   ```
   This is required to run the test cases and examples. If you are deploying your application, you can also copy the `kuzu_shared.dll` file to the same directory as your executable or to a directory that is already in the `PATH`.

For an example of how to properly set up the environment, you can also refer to our CI configuration file [here](.github/workflows/go.yml).

## Contributing
We welcome contributions to go-kuzu. By contributing to go-kuzu, you agree that your contributions will be licensed under the [MIT License](LICENSE). Please read the [contributing guide](CONTRIBUTING.md) for more information.

