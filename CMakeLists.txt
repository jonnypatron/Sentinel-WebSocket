cmake_minimum_required(VERSION 3.8)
project(gui_image_streamer)
 
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)   # necessário para websocketpp/asio
 
add_executable(gui_image_streamer src/imageStreamer.cpp)
 
ament_target_dependencies(gui_image_streamer rclcpp sensor_msgs)
 
target_include_directories(gui_image_streamer PRIVATE
  ${Boost_INCLUDE_DIRS}                          # headers do Boost/Asio
)
 
target_link_libraries(gui_image_streamer
  nlohmann_json::nlohmann_json
  Boost::system                                  # biblioteca Boost.System
  pthread
)
 
install(TARGETS gui_image_streamer DESTINATION lib/${PROJECT_NAME})
ament_package()