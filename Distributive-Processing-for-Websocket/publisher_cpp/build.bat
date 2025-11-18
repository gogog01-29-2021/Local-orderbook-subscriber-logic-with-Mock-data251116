@echo off
cd /d "%~dp0"
call "C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64

cl.exe /nologo /Zi /EHsc /std:c++17 /MD ^
  /DWIN32_LEAN_AND_MEAN ^
  /D_WIN32_WINNT=0x0A00 ^
  /DBOOST_ERROR_CODE_HEADER_ONLY ^
  /I "C:\vcpkg\installed\x64-windows\include" ^
  main.cpp ^
  /Fe:orderbook_rt.exe ^
  /link ^
  /LIBPATH:C:\vcpkg\installed\x64-windows\lib ^
  /NODEFAULTLIB:libcpmt ^
  libssl.lib ^
  libcrypto.lib ^
  Ws2_32.lib ^
  Crypt32.lib ^
  aws-cpp-sdk-kinesis.lib ^
  aws-cpp-sdk-core.lib
