@echo off
REM Build script for subscriber5.cpp - AWS Kinesis Consumer
REM Requires: Visual Studio Build Tools, vcpkg with AWS SDK and dependencies

cd /d "%~dp0"

REM Set up MSVC environment (adjust path if needed)
call "C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64

echo Building subscriber5.cpp...

cl.exe /nologo /Zi /EHsc /std:c++17 /MD ^
  /DWIN32_LEAN_AND_MEAN ^
  /D_WIN32_WINNT=0x0A00 ^
  /DBOOST_ERROR_CODE_HEADER_ONLY ^
  /I "C:\vcpkg\installed\x64-windows\include" ^
  subscriber5.cpp ^
  /Fe:subscriber5.exe ^
  /link ^
  /LIBPATH:C:\vcpkg\installed\x64-windows\lib ^
  /NODEFAULTLIB:libcpmt ^
  aws-cpp-sdk-kinesis.lib ^
  aws-cpp-sdk-core.lib ^
  Ws2_32.lib ^
  Crypt32.lib

if %ERRORLEVEL% == 0 (
  echo.
  echo Build successful! subscriber5.exe created.
  echo.
  echo To run, ensure DLLs are in PATH or copy them to this directory:
  echo   C:\vcpkg\installed\x64-windows\bin\aws-*.dll
  echo   C:\vcpkg\installed\x64-windows\bin\libssl-*.dll
  echo   C:\vcpkg\installed\x64-windows\bin\libcrypto-*.dll
  echo.
) else (
  echo.
  echo Build failed with error code %ERRORLEVEL%
  echo.
)

pause
