package consulx

import (
	"net"
	"os"
)

/********************************************************************
created:    2020-06-02
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

var localIp string

func init() {
	localIp = fetchLocalIp()
}

func GetLocalIp() string {
	return localIp
}

func fetchLocalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		_, _ = os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				var ip = ipnet.IP.String()
				return ip
			}
		}
	}

	return ""
}
