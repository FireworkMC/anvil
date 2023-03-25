# anvil

[![Go](https://github.com/FireworkMC/anvil/actions/workflows/go.yml/badge.svg)](https://github.com/FireworkMC/anvil/actions/workflows/go.yml) [![Go Report Card](https://goreportcard.com/badge/github.com/FireworkMC/anvil)](https://goreportcard.com/report/github.com/FireworkMC/anvil) [![codecov](https://codecov.io/gh/FireworkMC/anvil/branch/master/graph/badge.svg?token=LZZGDGKSII)](https://codecov.io/gh/FireworkMC/anvil) [![Go Reference](https://pkg.go.dev/badge/github.com/FireworkMC/anvil.svg)](https://pkg.go.dev/github.com/FireworkMC/anvil)

anvil is a simple library for reading and writing minecraft [anvil](https://minecraft.fandom.com/wiki/Anvil_file_format) files.

## Installation

```sh
go get github.com/FireworkMC/anvil
```

## Usage

### Reading and writing data from a directory containing anvil files

```go
a, err := anvil.Open("/path/to/anvil/dir")
if err != nil{
    // handle error
}

var buffer bytes.Buffer
_, err = a.Read(chunkX, chunkZ, &buffer)

// do stuff with the buffer


err = a.Write(chunkX, chunkZ, buffer.Bytes())

err = a.Close()

```

The `anvil.Anvil` returned by `Open` contains a cache of opened anvil files.
It is recommended to use `Open` instead of `OpenFile` since opening anvil files is an expensive operation.

### Reading and writing data from a single anvil file

```go
f, err := anvil.OpenFile("/path/to/anvil/file")
if err != nil{
    // handle error
}

var buffer bytes.Buffer
// relative coordinates of the chunk data must be used
// If the chunk exists at chunkX, chunkZ: chunkX % 32, chunkZ % 32 should be used. 
_, err = f.Read(chunkX%32, chunkZ%32, &buffer)


// do stuff with buffer

_, err = f.Write(chunkX%32, chunkZ%32, buffer.Bytes())

```
