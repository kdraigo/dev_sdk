package main

import (
	"crypto/ed25519"
	"crypto/rand"

	"github.com/sirupsen/logrus"
)

func main() {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to generate key.")
	}

	logrus.Infof("Public Key: %x", pubKey)
	logrus.Infof("Private Key: %x", privKey)
}
