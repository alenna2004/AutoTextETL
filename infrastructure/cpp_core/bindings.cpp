#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "text_engine.cpp"

namespace py = pybind11;

PYBIND11_MODULE(text_engine, m) {
    m.doc() = "C++ text processing module for ETL pipeline";

    m.def("split_by_delimiter", &split_by_delimiter,
        py::arg("text"), py::arg("delimiter"),
        "Split text by delimiter with C++ performance"
    );
}