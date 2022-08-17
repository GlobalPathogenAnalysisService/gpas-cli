## How to build Read-It-And-Keep and Samtools under Windows

Build of Read-It-And-Keep and Samtools for Windows is performed using [MSYS2](https://www.msys2.org/) tool which provides common GNU utilities (gcc, make, etc..) under Windows system.

1.  Install [MSYS2](https://www.msys2.org/) using instructions from its website.

2.  Open MSYS2 terminal (Windows Start > MSYS2 64bit > MSYS2 MSYS).

3.  Upgrade the system:
    ```shell
    pacman -Syu
    ```
    MSYS2 terminal may want to close at the end. So, close it and open it again. Update the rest with the following command:
    ```shell
    pacman -Su
    ```
4.  Install the following tools and libs:
    ```shell
    pacman -S gcc make base-devel zlib zlib-devel
    ```
5.  Install git:
    ```shell
    pacman -S git
    ```
6.  Go to the directory where you want to keep `read-it-and-keep` and `samtools` project. (In MSYS2 terminal to reference Windows disk start its name with slash. For example, to to the disk `'C'` root folder execute `cd /c/` command.)
    ```shell
    cd /c/Users/<user1>/projects
    ```
7.  Create a folder where some temporary files will be kept:
    ```shell
    mkdir temp
    ```
8.  Clone `read-it-and-keep` and `samtools` repositories from GitHub:
    ```shell
    git clone https://github.com/GenomePathogenAnalysisService/read-it-and-keep.git
    git clone https://github.com/samtools/samtools.git
    ```
9.  Perform a build of `read-it-and-keep` and `samtools` as it is described in their appropriate `README` files. For both repositories the build process is organized around `make` command.

10. Find built executables of each tool where they should appear according to their `README`s.

11. Test each tool right in MSYS2 terminal by invoking appropriate binary with `--help` argument.

12. To use `read-it-and-keep` and `samtools` executables outside of MSYS2 terminal copy the following DLLs from `C:\msys64\usr\bin` and keep them beside the executable:
    *  For `read-it-and-keep`:
       *  `msys-2.0.dll`
       *  `msys-gcc_s-seh-1.dll`
       *  `msys-stdc++-6.dll`
       *  `msys-z.dll`
    *  For `samtools`:
       *  `libbrotlicommon.dll`
       *  `libbrotlidec.dll`
       *  `libbz2-1.dll`
       *  `libcrypto-1_1-x64.dll`
       *  `libcurl-4.dll`
       *  `libiconv-2.dll`
       *  `libidn2-0.dll`
       *  `libintl-8.dll`
       *  `liblzma-5.dll`
       *  `libnghttp2-14.dll`
       *  `libpsl-5.dll`
       *  `libssh2-1.dll`
       *  `libssl-1_1-x64.dll`
       *  `libsystre-0.dll`
       *  `libtre-5.dll`
       *  `libunistring-2.dll`
       *  `libwinpthread-1.dll`
       *  `libzstd.dll`
       *  `zlib1.dll`

