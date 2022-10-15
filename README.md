# anvil

anvil is a simple library for reading and writing minecraft [anvil](https://minecraft.fandom.com/wiki/Anvil_file_format) files.

## Installation

```sh
go get github.com/FireworkMC/anvil
```

## Usage

### Reading chunk data from a directory containing anvil files

```go
a, err := anvil.Open("/path/to/anvil/dir")
if err != nil{
    // handle error
}

var buffer bytes.Buffer
_, err = a.Read(chunkX, chunkZ, &buffer)

```

The `anvil.Anvil` returned by `Open` contains a cache of opened anvil files.
It is recommended to use `Open` instead of `OpenFile` since opening anvil files is an expensive operation.

### Reading chunk data from a single anvil file

```go
f, err := anvil.OpenFile("/path/to/anvil/file")
if err != nil{
    // handle error
}

var buffer bytes.Buffer
// relative coordinates of the chunk data must be used
// If the chunk exists at chunkX, chunkZ: chunkX % 32, chunkZ % 32 should be used. 
_, err = f.Read(chunkX%32, chunkZ%32, &buffer)

```
