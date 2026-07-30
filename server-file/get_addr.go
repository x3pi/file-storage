package main
import (
    "fmt"
    "github.com/ethereum/go-ethereum/crypto"
)
func main() {
    privateKey, _ := crypto.HexToECDSA("e6b400585f8e1df3bca8302b7657249046d40c8ed92dba9a057287e4beca587a")
    address := crypto.PubkeyToAddress(privateKey.PublicKey)
    fmt.Println(address.Hex())
}
