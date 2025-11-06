using MddsSaver.Core.Shared.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MddsSaver.Infrastructure.Shared.Extensions
{
    public static class HostExtensions
    {
        public static void EnsureNetworkConnectivity(this IHost host)
        {
            var services = host.Services;
            var appSetting = services.GetRequiredService<AppSetting>();
            var configuration = host.Services.GetRequiredService<IConfiguration>();
            var logger = host.Services.GetRequiredService<ILogger<IHost>>();

            var kafkaEndpoints = GetEndpointFromBootstrapServers(configuration);
            var redisEndpoint = GetEndpointFromConnectionString(appSetting.Redis.Endpoint_Sentinel);
            var rabbitMqEndpoint = (appSetting.RabbitMQ.HostName, appSetting.RabbitMQ.Port);

            var parallelCheckEndpoints = kafkaEndpoints
                .Concat(new[] { redisEndpoint })
                .ToArray();

            CheckAnyEndpointReachableParallel(logger, parallelCheckEndpoints);
            CheckEndpointReachable(logger, rabbitMqEndpoint);
        }
        private static void CheckAnyEndpointReachableParallel(ILogger logger, params (string Host, int Port)[] endpoints)
        {
            logger.LogInformation($"{DateTime.Now:dd/MM/yyyy HH:mm:ss.fff} : Checking network => {LogEndPoints(endpoints)}");
            var retries = 3;
            var policy = Policy
                .Handle<Exception>()
                .Retry(retries, (ex, retry) =>
                {
                    logger.LogWarning($"{DateTime.Now:dd/MM/yyyy HH:mm:ss.fff} - Retry [{retry}/{retries}]: {ex.Message}");
                });
            var result = policy.ExecuteAndCapture(async () =>
            {
                //Tạo các task để connect ứng với mỗi IP
                var tasks = endpoints.Select(endpoint => Task.Run(() =>
                {
                    try
                    {
                        using var client = new TcpClient();
                        // thực hiện kiểm tra network
                        // TCPClient.Connect sẽ thực hiện mở kết nối đến endpoint trong 21s (timeout này phụ thuộc vào OS, trên windows là 21s)
                        // Nếu có exception, 
                        // không cần kiểm tra client.Connected, chỉ cần kiểm tra xem có exception hay không vì kể cả có network nhưng client.Connected vẫn trả về false.
                        // VD: đã kết nối nhưng sau đó bị endpoint từ chối
                        client.Connect(endpoint.Host, endpoint.Port);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }

                })).ToList();

                // Tiến hành kiểm tra từng task
                while (tasks.Any())
                {
                    // Chờ task đầu tiên hoàn thành
                    var finished = await Task.WhenAny(tasks);

                    // Nếu task đó thành công (trả về true) => return ngay
                    if (finished.Result)
                        return;

                    // Nếu task đầu tiên complete trả về false => Xóa nó và kiểm tra các task khác
                    tasks.Remove(finished);
                }

                // Nếu tất cả các task đều trả về false, throw exception để retry
                throw new Exception($"Check Connect to endPoints fail to => {LogEndPoints(endpoints)}");
            });
            //Xử lý sau khi kiểm tra
            if (result.Outcome.Equals(OutcomeType.Successful))
            {
                logger.LogInformation($"{DateTime.Now:dd/MM/yyyy HH:mm:ss.fff}: Network is ready => {LogEndPoints(endpoints)}");
                return;
            }
            else
            {
                //Connect thất bại thì exit app ngay
                logger.LogError($"{DateTime.Now:dd/MM/yyyy HH:mm:ss.fff}: Network check {LogEndPoints(endpoints)} failed with exception. \n{result.FinalException}");
                Environment.Exit(1);
            }
        }
        private static void CheckEndpointReachable(ILogger logger, (string Host, int Port) endpoint)
        {
            using (var client = new TcpClient())
            {
                // tạo policy để retry khi có exception, retry 3 lần, log exception ra console
                var retries = 3;
                var polly = Policy
                    .Handle<Exception>()
                    .Retry(retries, (ex, retry) => logger.LogWarning($"{DateTime.Now:dd/MM/yyyy HH:mm:ss.fff} - [{retry}/{retries}]: {ex.Message}"));

                logger.LogInformation($"{DateTime.Now:dd/MM/yyyy HH:mm:ss.fff}: Checking network, endpoint {endpoint.Host}:{endpoint.Port}");
                // thực hiện kiểm tra network
                // TCPClient.Connect sẽ thực hiện mở kết nối đến endpoint trong 21s (timeout này phụ thuộc vào OS, trên windows là 21s)
                // Nếu có exception, policy sẽ retry lại 3 lần
                // không cần kiểm tra client.Connected, chỉ cần kiểm tra xem có exception hay không vì kể cả có network nhưng client.Connected vẫn trả về false.
                // VD: đã kết nối nhưng sau đó bị endpoint từ chối
                var result = polly.ExecuteAndCapture(() => client.Connect(endpoint.Host, endpoint.Port));
                // nếu kết nối thành công, log ra console
                // nếu kết nối thất bại, log ra console exception và exit app
                if (result.Outcome.Equals(OutcomeType.Successful))
                    logger.LogInformation($"{DateTime.Now:dd/MM/yyyy HH:mm:ss.fff}: Network is ready.");
                else
                {
                    logger.LogError($"{DateTime.Now:dd/MM/yyyy HH:mm:ss.fff}: Network check failed with exception. \n{result.FinalException}");
                    Environment.Exit(1);
                }
            }
        }
        /// <summary>
        /// Get endpoint from configuration KafkaLogger.
        /// </summary>
        /// <param name="configuration">The application configuration.</param>
        /// <returns>A tuple containing the host and port.</returns>
        /// <exception cref="InvalidOperationException">Thrown if the configuration is missing.</exception>
        /// <exception cref="ArgumentException">Thrown if the port in the configuration is invalid.</exception>
        private static (string Host, int Port)[] GetEndpointFromBootstrapServers(IConfiguration configuration)
        {
            List<(string Host, int Port)> result = new();
            var bootstrapserver = configuration["KafkaLogger:BootstrapServers"];
            if (string.IsNullOrEmpty(bootstrapserver))
            {
                throw new InvalidOperationException("Kafka bootstrap servers are not configured (KafkaLogger:BootstrapServers).");
            }

            var endpoints = bootstrapserver.Split(',');
            foreach (var item in endpoints)
            {
                var parts = item.Split(':');
                var host = parts[0];
                if (parts.Length == 1)
                    result.Add((host, 9092));
                else
                {
                    if (!int.TryParse(parts[1], out int port))
                        throw new ArgumentException("Invalid port. Value: " + parts[1]);

                    result.Add((host, port));
                }
            }
            return result.ToArray();
        }
        private static (string Host, int Port) GetEndpointFromConnectionString(string connectionString)
        {
            // ToUpper hết vì phần host và port có thể viết hoa hoặc thường
            connectionString = connectionString.ToUpper();

            // 1. PostgreSQL: Host=...;Port=...
            var pgpattern = @"HOST=(?<host>[^;]+);PORT=(?<port>\d+)";
            var pgMatch = Regex.Match(connectionString, pgpattern);
            if (pgMatch.Success)
            {
                string host = pgMatch.Groups["host"].Value;
                string portStr = pgMatch.Groups["port"].Value;
                if (!int.TryParse(portStr, out int port))
                    throw new ArgumentException("Invalid port. Value: " + portStr);
                return (host, port);
            }
            // 2. Redis: host:port,options...
            // -> Lấy đoạn trước dấu phẩy đầu tiên
            var redisPart = connectionString.Split(',')[0];
            var redisPattern = @"(?<host>[^:]+):(?<port>\d+)";
            var redisMatch = Regex.Match(redisPart, redisPattern);
            if (redisMatch.Success)
            {
                string host = redisMatch.Groups["host"].Value;
                string portStr = redisMatch.Groups["port"].Value;
                if (!int.TryParse(portStr, out int port))
                    throw new ArgumentException("Invalid port. Value: " + portStr);

                return (host, port);
            }

            throw new ArgumentException("Invalid connection string. Value: " + connectionString);
        }
        private static string LogEndPoints(params (string Host, int Port)[] endpoints)
        {
            if (endpoints == null || endpoints.Length == 0) return string.Empty;
            return string.Join(",", endpoints.Select(x => $"{x.Host}:{x.Port}"));
        }
    }
}
