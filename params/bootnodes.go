// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package params

import "github.com/dominant-strategies/go-quai/common"

// ColosseumBootnodes are the enode URLs of the P2P bootstrap nodes running on
// the Colosseum test network.
var ColosseumBootnodes = []string{
	"enode://1edee38aebefea9150213cdf4306a64499e3bca9058971c443d31a4997b9a096e7a26059472529a6ccbca0a04c4f6fdbf0c880ee97545a12c0f8b656f7d4e7da@35.192.126.165",
	"enode://0f7b74f957d68eb85bbbf8ffe7d0477575406beb647f2353de08ca291a03db1507a70a41f3b4623c3049512342781c8fa9a7f7654cbcc8603e51190430b839a6@34.170.190.144",
	"enode://66c87db18ab3e6322a149ac4b7b1bde3a31f1e505945a358e4c1ebae9dc8a40402ccd96dfed35438448fbf79a7b8ee093581a05c40ab115adaebe81a6946993b@104.197.16.107",
}

// GardenBootnodes are the enode URLs of the P2P bootstrap nodes running on the
// Garden test network.
var GardenBootnodes = []string{
	"enode://87370da8b875f8e865904f6c3e6d6f78a0392ca3c88fbde291c8ade8b61849de217fa8ab87b5f7aa0ac86b4b473f19be5899e1840edc0846b0150375f80708e6@34.64.47.60",   // Asia
	"enode://cc28d297a8ba6f80e979a7006d2d92131c6b669688889ee1d75c5a987b92bc8be339d0706900e485f4751736844267754619bec11d50be7d7ba4d023ba8662f0@34.175.50.212", // Europe
	"enode://7162812dacb39a8ad7d9c28d7ffed030e88b07b44a8da84ce8898f8df0548e173ff0771a5b3dd58e331c286ae58c67824b9684aab08158e2340e70217a09bd28@34.70.251.243", // Central USA
	"enode://1bf0390882f96086d0f818ffdad5bce96103767fa56cda468509c81cb4f3c65f1c85472b94725eae7d4edba24deffae20c8d70e0201242ecfee738e06592fc6e@34.95.212.143", // SouthAmerica
}

// OrchardBootnodes are the enode URLs of the P2P bootstrap nodes running on the
// Orchard test network
var OrchardBootnodes = []string{
	"enode://5f03bfac8b38f18e7384d72e48bb6f9a4970ce75b3a36b45041cc1bfde61e2b760b46d9d73e2a40806ccb07f1fb32e9c38d350829c03ed9ba16369c9c24c3664@34.64.202.127",
	"enode://e52f659870791a0358e81db559d5fd257c753a5c6bf483e62922980b50d289f54b1efb6f49e1e20ea4d635ed76681163849fae0a2bd16d2f40ed72332487946a@34.175.158.49",
	"enode://44facffd8d3376d93ac839d9762f830f27ef1d400bf1d7106e7997b1bd2c476bf439c14bcd125e263163e571ddb803b01ef25689aa0d43d50496f4136bcd4710@34.95.228.160",
}

var V5Bootnodes = []string{}

const dnsPrefix = "enrtree://ALE24Z2TEZV2XK46RXVB6IIN5HB5WTI4F4SMAVLYCAQIUPU53RSUU@"

// KnownDNSNetwork returns the address of a public DNS-based node list for the given
// genesis hash and protocol. See https://github.com/ethereum/discv4-dns-lists for more
// information.
func KnownDNSNetwork(genesis common.Hash, protocol string) string {
	var net string
	switch genesis {
	case ColosseumGenesisHash:
		net = "colosseum"
	case GardenGenesisHash:
		net = "garden"
	case OrchardGenesisHash:
		net = "orchard"
	default:
		return ""
	}
	return dnsPrefix + common.NodeLocation.Name() + "." + net + ".quainodes.io"
}
