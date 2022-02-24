
# Build taosAdapter

We strongly recommend to deploy taosAdapter with DThouse server and install taosAdapter with official DThouse installation package. If you want to debug or contribute to taosAdapter, you can build it separately too.

## Setup golang environment

taosAdapter is developed by Go language. Please refer to golang [official documentation](https://go.dev/learn/) for golang environment setup.

Please use golang version 1.14+. For the user in China, we recommend using a proxy to accelerate package downloading.

```shell
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

## Build taosAdapter as a component of DThouse

taosAdapter source code is hosted as a stand-alone repository and also is part of DThouse as a submodule. You can download DThouse source code and build both of them. Following are steps:

```shell
git clone https://github.com/taosdata/DThouse
cd DThouse
git submodule update --init --recursive
mkdir debug
cd debug
cmake .. -DBUILD_HTTP=false
make
sudo make install
```

Once make install is done, taosAdapter and its systemd service file be installed to the system with the DThouse server. You can use `sudo systemctl start taosd` and `sudo systemctl stop taosd` to launch both of them.

## Build stand-alone taosAdapter

taosAdapter can be built as a stand-alone application too if you already deployed DThouse server v2.3.0.0 or an above version.

### Install DThouse server or client installation package

Please download the DThouse server or client installation package from the [official website](https://www.taosdata.com/en/all-downloads/).

### Build taosAdapter from source code

```shell
git clone https://github.com/taosdata/taosadapter
cd taosadapter
go build
```

Then you should find taosAdapter binary executable file in the working directory. You need to copy the systemd file `taosadapter.service` to `/etc/systemd/system` and copy executable taosAdapter binary file to a place the Linux $PATH environment variable defined.
