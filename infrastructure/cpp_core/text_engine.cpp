#include <vector>
#include <string>
#include <algorithm>

std::vector<std::string> split_by_delimiter(const std::string& text, const std::string& delimiter) {
    std::vector<std::string> result;
    size_t start = 0, end = 0;

    while ((end = text.find(delimiter, start)) != std::string::npos) {
        result.push_back(text.substr(start, end - start));
        start = end + delimiter.length();
    }
    result.push_back(text.substr(start));
    return result;
}