cmake_minimum_required(VERSION 3.12)
project(etl_cpp_core)

# Находим pybind11
find_package(pybind11 REQUIRED)

# Компилируем модуль text_engine
pybind11_add_module(text_engine
    text_engine.cpp
    bindings.cpp
)

# Компилируем модуль pdf_parser
pybind11_add_module(pdf_parser
    pdf_parser_core.cpp
    pdf_bindings.cpp
)

# Устанавливаем в правильную директорию
install(TARGETS text_engine pdf_parser
    LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/infrastructure/cpp_core
)