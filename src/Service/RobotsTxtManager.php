<?php

namespace App\Service;

use GuzzleHttp\Client as GuzzleClient;
use Psr\Log\LoggerInterface;

class RobotsTxtManager
{
    private GuzzleClient $httpClient;
    private LoggerInterface $logger;
    private array $cache = [];
    private array $lastRequestTime = [];
    private string $userAgent = 'ODIS https://search.odis.org';

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->httpClient = new GuzzleClient([
            'timeout'  => 10.0,
            'verify' => false,
            'headers' => [
                'User-Agent' => $this->userAgent,
            ]
        ]);
    }

    public function isAllowed(string $url): bool
    {
        $parsedUrl = parse_url($url);
        if (!isset($parsedUrl['host']) || !isset($parsedUrl['scheme'])) {
            return true;
        }

        $host = $parsedUrl['host'];
        $scheme = $parsedUrl['scheme'];
        $path = $parsedUrl['path'] ?? '/';
        if (isset($parsedUrl['query'])) {
            $path .= '?' . $parsedUrl['query'];
        }

        $rules = $this->getRules($scheme, $host);
        
        // Check Allow rules first (standard robots.txt behavior)
        foreach ($rules['allow'] as $allowedPath) {
            if ($this->matchPath($path, $allowedPath)) {
                return true;
            }
        }

        // Check Disallow rules
        foreach ($rules['disallow'] as $disallowedPath) {
            if ($this->matchPath($path, $disallowedPath)) {
                return false;
            }
        }

        return true;
    }

    public function getDelay(string $url): int
    {
        $parsedUrl = parse_url($url);
        if (!isset($parsedUrl['host']) || !isset($parsedUrl['scheme'])) {
            return 0;
        }

        $host = $parsedUrl['host'];
        $scheme = $parsedUrl['scheme'];
        $rules = $this->getRules($scheme, $host);

        return $rules['crawl-delay'] ?? 0;
    }

    public function waitIfNecessary(string $url): void
    {
        $parsedUrl = parse_url($url);
        if (!isset($parsedUrl['host'])) {
            return;
        }

        $host = $parsedUrl['host'];
        $delay = $this->getDelay($url);

        if ($delay > 0 && isset($this->lastRequestTime[$host])) {
            $elapsed = microtime(true) - $this->lastRequestTime[$host];
            if ($elapsed < $delay) {
                $waitTime = (int)(($delay - $elapsed) * 1000000);
                $this->logger->debug("Waiting $waitTime microseconds for robots.txt Crawl-delay on $host");
                usleep($waitTime);
            }
        }

        $this->lastRequestTime[$host] = microtime(true);
    }

    private function getRules(string $scheme, string $host): array
    {
        $cacheKey = $scheme . '://' . $host;
        if (isset($this->cache[$cacheKey])) {
            return $this->cache[$cacheKey];
        }

        $rules = [
            'allow' => [],
            'disallow' => [],
            'crawl-delay' => 0,
        ];

        try {
            $robotsUrl = $scheme . '://' . $host . '/robots.txt';
            $response = $this->httpClient->get($robotsUrl);
            if ($response->getStatusCode() === 200) {
                $content = (string)$response->getBody();
                $rules = $this->parseRobotsTxt($content);
            }
        } catch (\Exception $e) {
            // If robots.txt is missing or error, assume everything is allowed
            $this->logger->debug("Could not fetch robots.txt for $host: " . $e->getMessage());
        }

        $this->cache[$cacheKey] = $rules;
        return $rules;
    }

    private function parseRobotsTxt(string $content): array
    {
        $rules = [
            'allow' => [],
            'disallow' => [],
            'crawl-delay' => 0,
        ];

        $lines = explode("\n", $content);
        $userAgentApplies = false;
        
        foreach ($lines as $line) {
            $line = trim(preg_replace('/#.*$/', '', $line));
            if (empty($line)) continue;

            if (preg_match('/^User-agent:\s*(.*)$/i', $line, $matches)) {
                $ua = trim($matches[1]);
                if ($ua === '*' || stripos($this->userAgent, $ua) !== false) {
                    $userAgentApplies = true;
                } else {
                    $userAgentApplies = false;
                }
                continue;
            }

            if (!$userAgentApplies) continue;

            if (preg_match('/^Disallow:\s*(.*)$/i', $line, $matches)) {
                $path = trim($matches[1]);
                if (!empty($path)) {
                    $rules['disallow'][] = $path;
                }
            } elseif (preg_match('/^Allow:\s*(.*)$/i', $line, $matches)) {
                $path = trim($matches[1]);
                if (!empty($path)) {
                    $rules['allow'][] = $path;
                }
            } elseif (preg_match('/^Crawl-delay:\s*([0-9\.]+)$/i', $line, $matches)) {
                $rules['crawl-delay'] = (float)$matches[1];
            }
        }

        return $rules;
    }

    private function matchPath(string $path, string $pattern): bool
    {
        // Simple prefix match for now, could be improved with regex
        $pattern = str_replace(['*', '?'], ['.*', '\?'], $pattern);
        $pattern = '#^' . $pattern . '#';
        return (bool)preg_match($pattern, $path);
    }
}
