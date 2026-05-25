#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/compressed_image.hpp"

#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#include <nlohmann/json.hpp>

#include <atomic>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

typedef websocketpp::server<websocketpp::config::asio> Server;
using websocketpp::connection_hdl;
using json = nlohmann::json;

#define GREEN  "\033[1;32m"
#define RED    "\033[1;31m"
#define YELLOW "\033[1;33m"
#define CYAN   "\033[1;36m"
#define CLEAR  "\033[0m"

// Macros de logging — só funcionam dentro da classe (usam this->get_logger())
#define INFO(msg)  RCLCPP_INFO_STREAM (this->get_logger(), CYAN   << msg << CLEAR)
#define OK(msg)    RCLCPP_INFO_STREAM (this->get_logger(), GREEN  << msg << CLEAR)
#define WARN(msg)  RCLCPP_WARN_STREAM (this->get_logger(), YELLOW << msg << CLEAR)
#define ERROR(msg) RCLCPP_ERROR_STREAM(this->get_logger(), RED    << msg << CLEAR)

// ── Data Structures ──────────────────────────────────────────────────────────

struct ClientState {
  int         id;
  std::string activeTopic;
};

struct TopicState {
  // ROS2: SharedPtr em vez de ros::Subscriber
  rclcpp::Subscription<sensor_msgs::msg::CompressedImage>::SharedPtr subscription;
  std::set<connection_hdl, std::owner_less<connection_hdl>> clients;
};

// ── ImageStreamer Node ────────────────────────────────────────────────────────

class ImageStreamer : public rclcpp::Node   // ← herda de rclcpp::Node
{
public:
  ImageStreamer(int argc, char **argv);
  ~ImageStreamer();

private:
  std::atomic<int> nextClientID{1};
  std::map<connection_hdl, ClientState, std::owner_less<connection_hdl>> clientStates;
  std::map<std::string, TopicState> topicStates;
  std::mutex stateMutex;

  Server            wsServer;
  std::thread       wsThread;
  std::atomic<bool> wsRunning{false};
  int               socketPort{9092};

  int  getClientID(connection_hdl hdl);
  void messageHandler  (connection_hdl hdl, Server::message_ptr msg);
  void handleStreamEnable (connection_hdl hdl, const std::string &topic);
  void handleStreamDisable(connection_hdl hdl, const std::string &topic);
  void handleStreamSwitch (connection_hdl hdl, const std::string &newTopic);
  void imageCallback(sensor_msgs::msg::CompressedImage::ConstSharedPtr msg,
                     const std::string &topic);
};

// ── Constructor ───────────────────────────────────────────────────────────────

ImageStreamer::ImageStreamer(int argc, char **argv)
  : rclcpp::Node("gui_image_streamer")   // ← inicializa Node
{
  // Argumento CLI para a porta
  if (argc >= 2) {
    try   { socketPort = std::stoi(argv[1]); }
    catch (const std::exception &e) {
      WARN("[ImageStreamer] Porta CLI inválida '" << argv[1] << "': " << e.what());
    }
  }

  // Parâmetro ROS2 (sobrepõe CLI se definido)
  this->declare_parameter("socket_port", socketPort);
  socketPort = this->get_parameter("socket_port").as_int();

  // WebSocket Server
  INFO("[ImageStreamer] A iniciar na porta " << socketPort);
  wsServer.init_asio();
  wsServer.set_reuse_addr(true);
  wsServer.clear_access_channels(websocketpp::log::alevel::all);
  wsServer.clear_error_channels(websocketpp::log::elevel::all);

  wsServer.set_open_handler([this](connection_hdl hdl) {
    int id = nextClientID++;
    std::size_t total;
    { std::lock_guard<std::mutex> lock(stateMutex);
      clientStates[hdl] = ClientState{id, ""}; total = clientStates.size(); }
    INFO("[ImageStreamer] Cliente " << id << " ligado. Total: " << total);
  });

  wsServer.set_close_handler([this](connection_hdl hdl) {
    std::string activeTopic;
    int clientId = -1;
    std::size_t total = 0, remaining = 0;
    rclcpp::Subscription<sensor_msgs::msg::CompressedImage>::SharedPtr subToReset;

    { std::lock_guard<std::mutex> lock(stateMutex);
      auto cIt = clientStates.find(hdl);
      if (cIt != clientStates.end()) {
        clientId = cIt->second.id; activeTopic = cIt->second.activeTopic;
        clientStates.erase(cIt);
      }
      total = clientStates.size();
      if (!activeTopic.empty()) {
        auto tIt = topicStates.find(activeTopic);
        if (tIt != topicStates.end()) {
          tIt->second.clients.erase(hdl);
          remaining = tIt->second.clients.size();
          if (tIt->second.clients.empty()) {
            subToReset = tIt->second.subscription;
            topicStates.erase(tIt);
          }
        }
      }
    }

    subToReset.reset(); // ← ROS2: reset do SharedPtr = unsubscribe

    if (!activeTopic.empty())
      INFO("[ImageStreamer] Cliente " << clientId << " desligado de '" << activeTopic
           << "'. Restantes: " << remaining << ". Total: " << total);
    else
      INFO("[ImageStreamer] Cliente " << clientId << " desligado. Total: " << total);
  });

  wsServer.set_message_handler([this](connection_hdl hdl, Server::message_ptr msg) {
    this->messageHandler(hdl, msg);
  });

  wsThread = std::thread([this]() {
    try {
      wsServer.listen(socketPort);
      wsServer.start_accept();
      wsRunning = true;
      wsServer.run();
    } catch (const std::exception &e) {
      ERROR("[ImageStreamer] Erro no servidor: " << e.what());
      rclcpp::shutdown(); // ← ROS2
    }
    wsRunning = false;
  });
}

// ── Destructor ────────────────────────────────────────────────────────────────

ImageStreamer::~ImageStreamer()
{
  std::vector<connection_hdl> openClients;
  std::vector<rclcpp::Subscription<sensor_msgs::msg::CompressedImage>::SharedPtr> subsToClear;

  { std::lock_guard<std::mutex> lock(stateMutex);
    for (const auto &e : clientStates)  openClients.push_back(e.first);
    for (auto &e : topicStates)         subsToClear.push_back(e.second.subscription);
    clientStates.clear();
    topicStates.clear();
  }

  subsToClear.clear(); // destrói subscriptions

  for (const auto &hdl : openClients) {
    try { wsServer.close(hdl, websocketpp::close::status::going_away, "shutdown"); }
    catch (...) {}
  }

  try { wsServer.stop_listening(); } catch (...) {}
  if (wsRunning) wsServer.stop();
  if (wsThread.joinable()) wsThread.join();
}

// ── Utility ───────────────────────────────────────────────────────────────────

int ImageStreamer::getClientID(connection_hdl hdl)
{
  std::lock_guard<std::mutex> lock(stateMutex);
  auto it = clientStates.find(hdl);
  return (it == clientStates.end()) ? -1 : it->second.id;
}

// ── Message Handler ───────────────────────────────────────────────────────────

void ImageStreamer::messageHandler(connection_hdl hdl, Server::message_ptr msg)
{
  if (msg->get_opcode() != websocketpp::frame::opcode::text) return;

  const std::string payload = msg->get_payload();
  if (payload.size() < 2) return;

  json j;
  try { j = json::parse(payload); }
  catch (...) { ERROR("[ImageStreamer] Erro a parsear JSON"); return; }

  if (!j.contains("cmd") || !j.contains("topic") ||
      !j["cmd"].is_string() || !j["topic"].is_string()) return;

  const std::string cmd = j["cmd"], topic = j["topic"];
  if (topic.empty() || topic[0] != '/') return;

  if      (cmd == "enable")  handleStreamEnable(hdl, topic);
  else if (cmd == "disable") handleStreamDisable(hdl, topic);
  else if (cmd == "switch")  handleStreamSwitch(hdl, topic);
}

// ── Stream Enable ─────────────────────────────────────────────────────────────

void ImageStreamer::handleStreamEnable(connection_hdl hdl, const std::string &topic)
{
  int clientId; std::size_t viewers;
  { std::lock_guard<std::mutex> lock(stateMutex);
    auto cIt = clientStates.find(hdl);
    if (cIt == clientStates.end()) return;
    clientId = cIt->second.id;
    if (!cIt->second.activeTopic.empty()) {
      WARN("[ImageStreamer] Cliente " << clientId << " já está a ver um tópico. Usa 'switch'.");
      return;
    }
    auto tIt = topicStates.find(topic);
    if (tIt == topicStates.end()) {
      TopicState ts;
      // ROS2: create_subscription com SensorDataQoS (best_effort, depth 1)
      ts.subscription = this->create_subscription<sensor_msgs::msg::CompressedImage>(
          topic, rclcpp::SensorDataQoS(),
          [this, topic](sensor_msgs::msg::CompressedImage::ConstSharedPtr m) {
            this->imageCallback(m, topic);
          });
      tIt = topicStates.emplace(topic, std::move(ts)).first;
    }
    tIt->second.clients.insert(hdl);
    cIt->second.activeTopic = topic;
    viewers = tIt->second.clients.size();
  }
  OK("[ImageStreamer] Cliente " << clientId << " -> '" << topic << "'. Viewers: " << viewers);
}

// ── Stream Disable ────────────────────────────────────────────────────────────

void ImageStreamer::handleStreamDisable(connection_hdl hdl, const std::string &topic)
{
  bool shouldReset = false;
  rclcpp::Subscription<sensor_msgs::msg::CompressedImage>::SharedPtr subToReset;
  int clientId; std::size_t viewers = 0;

  { std::lock_guard<std::mutex> lock(stateMutex);
    auto cIt = clientStates.find(hdl);
    if (cIt == clientStates.end() || cIt->second.activeTopic != topic) return;
    clientId = cIt->second.id;
    auto tIt = topicStates.find(topic);
    if (tIt != topicStates.end()) {
      tIt->second.clients.erase(hdl);
      viewers = tIt->second.clients.size();
      if (tIt->second.clients.empty()) {
        subToReset = tIt->second.subscription; shouldReset = true;
        topicStates.erase(tIt);
      }
    }
    cIt->second.activeTopic.clear();
  }

  if (shouldReset) subToReset.reset();
  OK("[ImageStreamer] Cliente " << clientId << " -> Disabled '" << topic << "'. Viewers: " << viewers);
}

// ── Stream Switch ─────────────────────────────────────────────────────────────

void ImageStreamer::handleStreamSwitch(connection_hdl hdl, const std::string &newTopic)
{
  std::string oldTopic;
  rclcpp::Subscription<sensor_msgs::msg::CompressedImage>::SharedPtr oldSubToReset;
  bool resetOld = false;
  int clientId; std::size_t newViewers = 0, oldViewers = 0;

  { std::lock_guard<std::mutex> lock(stateMutex);
    auto cIt = clientStates.find(hdl);
    if (cIt == clientStates.end()) return;
    clientId = cIt->second.id; oldTopic = cIt->second.activeTopic;
    if (oldTopic == newTopic) return;

    auto nIt = topicStates.find(newTopic);
    if (nIt == topicStates.end()) {
      TopicState ts;
      ts.subscription = this->create_subscription<sensor_msgs::msg::CompressedImage>(
          newTopic, rclcpp::SensorDataQoS(),
          [this, newTopic](sensor_msgs::msg::CompressedImage::ConstSharedPtr m) {
            this->imageCallback(m, newTopic);
          });
      nIt = topicStates.emplace(newTopic, std::move(ts)).first;
    }
    nIt->second.clients.insert(hdl); newViewers = nIt->second.clients.size();
    cIt->second.activeTopic = newTopic;

    if (!oldTopic.empty()) {
      auto oIt = topicStates.find(oldTopic);
      if (oIt != topicStates.end()) {
        oIt->second.clients.erase(hdl); oldViewers = oIt->second.clients.size();
        if (oIt->second.clients.empty()) {
          oldSubToReset = oIt->second.subscription; resetOld = true;
          topicStates.erase(oIt);
        }
      }
    }
  }

  if (resetOld) oldSubToReset.reset();
  OK("[ImageStreamer] Cliente " << clientId << " -> '"
     << (oldTopic.empty() ? "none" : oldTopic) << "' → '" << newTopic
     << "'. Viewers novo: " << newViewers << ", antigo: " << oldViewers);
}

// ── Image Callback ────────────────────────────────────────────────────────────

void ImageStreamer::imageCallback(
    sensor_msgs::msg::CompressedImage::ConstSharedPtr msg,
    const std::string &topic)
{
  std::vector<connection_hdl> interested;
  { std::lock_guard<std::mutex> lock(stateMutex);
    auto tIt = topicStates.find(topic);
    if (tIt == topicStates.end() || tIt->second.clients.empty()) return;
    for (const auto &hdl : tIt->second.clients) interested.push_back(hdl);
  }

  json header;
  header["topic"]     = topic;
  header["format"]    = msg->format;
  header["timestamp"] = msg->header.stamp.sec + msg->header.stamp.nanosec * 1e-9; // ← ROS2

  const std::string headerStr  = header.dump();
  const uint32_t    headerSize = static_cast<uint32_t>(headerStr.size());
  const std::size_t totalSize  = sizeof(headerSize) + headerStr.size() + msg->data.size();

  std::vector<uint8_t> frame(totalSize);
  std::memcpy(frame.data(),                                       &headerSize,       sizeof(headerSize));
  std::memcpy(frame.data() + sizeof(headerSize),                  headerStr.data(),  headerStr.size());
  std::memcpy(frame.data() + sizeof(headerSize) + headerStr.size(), msg->data.data(), msg->data.size());

  for (const auto &hdl : interested) {
    try {
      wsServer.send(hdl, frame.data(), frame.size(), websocketpp::frame::opcode::binary);
    } catch (const std::exception &e) {
      ERROR("[ImageStreamer] Falha ao enviar para cliente " << getClientID(hdl) << ": " << e.what());
    }
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char **argv)
{
  rclcpp::init(argc, argv);

  // ROS2: MultiThreadedExecutor (equivalente ao AsyncSpinner(2) do ROS1)
  rclcpp::executors::MultiThreadedExecutor executor(rclcpp::ExecutorOptions(), 2);
  auto node = std::make_shared<ImageStreamer>(argc, argv);
  executor.add_node(node);
  executor.spin();

  rclcpp::shutdown();
  return 0;
}