cmake_minimum_required(VERSION 3.16)
project(algotrade_demo_bot LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED)
find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(Threads REQUIRED)

add_executable(demo_bot demo_bot.cpp)

target_link_libraries(demo_bot PRIVATE
    Boost::boost
    nlohmann_json::nlohmann_json
    Threads::Threads
)
