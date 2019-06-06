package utils

import (
	"errors"
	"net"
)

func GetIP(inter string) (string, error) {
	ifi, err := net.InterfaceByName(inter)
	if err != nil {
		return "", err
	}

	addrs, err := ifi.Addrs()
	if err != nil {
		return "", err
	}

	if len(addrs) <= 0 {
		return "", errors.New("no IP.")
	}

	ipnet, ok := addrs[0].(*net.IPNet)

	if !ok {
		return "", errors.New("get ip error.")
	}

	return ipnet.IP.String(), nil
}
