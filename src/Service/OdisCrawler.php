<?php

namespace App\Service;

use Elastic\Elasticsearch\Client;
use GuzzleHttp\Client as GuzzleClient;
use Symfony\Component\DomCrawler\Crawler;
use Psr\Log\LoggerInterface;
use App\Entity\CrawlStat;
use Doctrine\ORM\EntityManagerInterface;

use Symfony\Component\Console\Output\OutputInterface;

class OdisCrawler
{
    private GuzzleClient $httpClient;
    private Client $esClient;
    private string $esIndex = 'odis_metadata';
    private string $recordsApiUrl = 'https://catalogue.odis.org/odis-arch-records';
    private string $viewBaseUrl = 'https://catalogue.odis.org/view/';
    private LoggerInterface $logger;
    private EntityManagerInterface $entityManager;

    private int $nodesFoundCount = 0;
    private int $pagesCrawledCount = 0;
    private int $validJsonLdsCount = 0;
    private int $invalidJsonLdsCount = 0;
    private int $crawlerErrorsCount = 0;
    private array $errorDetails = [];
    private int $maxStoredErrors = 50;
    private int $limit = 0;
    private int $processedInCurrentDatasource = 0;
    private ?OutputInterface $output = null;
    private ?CrawlStat $currentStat = null;
    private int $lastUpdateTimestamp = 0;
    private string $currentDatasourceId = '';
    private string $commandLine = '';
    private RobotsTxtManager $robotsManager;

    public function __construct(Client $esClient, LoggerInterface $logger, EntityManagerInterface $entityManager, RobotsTxtManager $robotsManager)
    {
        $this->esClient = $esClient;
        $this->logger = $logger;
        $this->entityManager = $entityManager;
        $this->robotsManager = $robotsManager;
        $this->httpClient = new GuzzleClient([
            'timeout'  => 15.0,
            'verify' => false,
        ]);
    }

    public function setOutput(?OutputInterface $output): void
    {
        $this->output = $output;
    }

    public function setCommandLine(string $commandLine): void
    {
        $this->commandLine = $commandLine;
    }

    public function setLimit(int $limit): void
    {
        $this->limit = $limit;
    }

    private function log(string $message, string $level = 'info'): void
    {
        // Don't log extremely long messages to avoid memory issues with Monolog
        if (strlen($message) > 5000) {
            $message = substr($message, 0, 5000) . '... (message truncated)';
        }

        $this->logger->$level($message);
        
        if ($this->output !== null) {
            $prefix = match($level) {
                'error' => '<error>[ERROR]</error> ',
                'warning' => '<comment>[WARN]</comment> ',
                'info' => '<info>[INFO]</info> ',
                default => "[DEBUG] ",
            };
            $this->output->writeln($prefix . $message);
        }
    }

    public function run(?array $specificIds = null, ?array $skipIds = null): void
    {
        gc_collect_cycles(); // Cleanup memory before start
        $this->nodesFoundCount = 0;
        $this->pagesCrawledCount = 0;
        $this->validJsonLdsCount = 0;
        $this->invalidJsonLdsCount = 0;
        $this->crawlerErrorsCount = 0;
        $this->errorDetails = [];
        $this->lastUpdateTimestamp = time();

        $this->currentStat = new CrawlStat();
        $this->currentStat->setStatus('in_progress');
        // If we are crawling specific IDs, it's a 'targeted' crawl
        if ($specificIds !== null && !empty($specificIds)) {
            $this->currentStat->setType('targeted');
        } else {
            $this->currentStat->setType('full');
        }
        $this->currentStat->setCommandLine($this->commandLine);
        $this->currentStat->setNodesFound(0);
        $this->currentStat->setPagesCrawled(0);
        $this->currentStat->setValidJsonLds(0);
        $this->currentStat->setInvalidJsonLds(0);
        $this->currentStat->setCrawlerErrors(0);
        $this->currentStat->setErrorDetails([]);
        $this->currentStat->setProcessedEntries([]);
        $this->entityManager->persist($this->currentStat);
        $this->entityManager->flush();

        try {
            $this->ensureIndexExists();
        } catch (\Exception $e) {
            $message = "Elasticsearch connection failed: " . $e->getMessage();
            $this->log($message, 'error');
            $this->crawlerErrorsCount++;
            $shortMessage = strlen($message) > 500 ? substr($message, 0, 500) . '...' : $message;
            $this->errorDetails[] = $shortMessage;
            $this->saveStats('failed');
            return;
        }

        if ($specificIds !== null && !empty($specificIds)) {
            $records = $this->getRecords();
            $dsIds = $specificIds;
            $this->log("Running targeted crawl for IDs: " . implode(', ', $dsIds));
            // For targeted crawls, we don't update nodesFound to avoid "decreasing" the global count on the dashboard cards
            // but we still want progress bar if we had one.
            // Let's set it to current max so it's stable in the cards.
            $maxNodes = $this->entityManager->getRepository(CrawlStat::class)->countUniqueNodesFound();
            $this->nodesFoundCount = max($maxNodes, count($dsIds));
        } else {
            $records = $this->getRecords();
            $dsIds = array_keys($records);
            $this->log("Found " . count($dsIds) . " datasource IDs");
            $this->nodesFoundCount = count($dsIds);
        }

        if ($skipIds !== null && !empty($skipIds)) {
            $this->log("Skipping IDs: " . implode(', ', $skipIds));
            $dsIds = array_filter($dsIds, fn($id) => !in_array($id, $skipIds));
            // Update nodes found count if we skipped some
            if ($specificIds === null) {
                $this->nodesFoundCount = count($dsIds);
            }
        }

        foreach ($dsIds as $id) {
            $record = $records[$id] ?? null;
            $this->processDatasource($id, $record);
            gc_collect_cycles(); // Cleanup memory after each datasource
        }

        $this->saveStats();
    }

    private function saveStats(string $status = 'completed'): void
    {
        if ($this->currentStat === null) {
            $this->currentStat = new CrawlStat();
            $this->entityManager->persist($this->currentStat);
        }

        // Final deduplication before finishing
        $this->errorDetails = array_values(array_unique($this->errorDetails));

        $this->currentStat->setNodesFound($this->nodesFoundCount);
        $this->currentStat->setPagesCrawled($this->pagesCrawledCount);
        $this->currentStat->setValidJsonLds($this->validJsonLdsCount);
        $this->currentStat->setInvalidJsonLds($this->invalidJsonLdsCount);
        $this->currentStat->setCrawlerErrors($this->crawlerErrorsCount);
        $this->currentStat->setErrorDetails($this->errorDetails);
        $this->currentStat->setStatus($status);

        if ($status === 'completed' || $status === 'failed') {
            $this->currentStat->setFinishedAt(new \DateTimeImmutable());
        }

        $this->entityManager->flush();

        if ($status === 'completed' || $status === 'failed') {
            $this->log(sprintf(
                "Crawl stats saved (%s): %d nodes, %d pages, %d valid JSON-LDs, %d invalid, %d errors",
                $status, $this->nodesFoundCount, $this->pagesCrawledCount, $this->validJsonLdsCount, $this->invalidJsonLdsCount, $this->crawlerErrorsCount
            ));
        }
    }

    private function updateProgress(): void
    {
        // Update DB every 2 seconds to avoid too many writes but keep it "real-time" enough
        if (time() - $this->lastUpdateTimestamp >= 2) {
            // Deduplicate and cap errorDetails right before saving
            $this->errorDetails = array_values(array_unique($this->errorDetails));
            if (count($this->errorDetails) > $this->maxStoredErrors) {
                $this->errorDetails = array_slice($this->errorDetails, 0, $this->maxStoredErrors);
                $this->errorDetails[] = "... and more (too many unique errors logged)";
                $this->errorDetails = array_values(array_unique($this->errorDetails));
            }

            $this->saveStats('in_progress');
            $this->lastUpdateTimestamp = time();
        }
    }

    private function ensureIndexExists(): void
    {
        $params = ['index' => $this->esIndex];
        if (!$this->esClient->indices()->exists($params)->asBool()) {
            $this->esClient->indices()->create([
                'index' => $this->esIndex,
                'body' => [
                    'mappings' => [
                        'properties' => [
                            'name' => [
                                'type' => 'text',
                                'fields' => ['keyword' => ['type' => 'keyword']]
                            ],
                            'description' => ['type' => 'text'],
                            'keywords' => ['type' => 'flattened'],
                            'knowsAbout' => ['type' => 'flattened'],
                            'contributor' => ['type' => 'flattened'],
                            'distribution' => ['type' => 'flattened'],
                            'identifier' => ['type' => 'flattened'],
                            '@type' => [
                                'type' => 'text',
                                'fields' => ['keyword' => ['type' => 'keyword']]
                            ],
                            'url' => ['type' => 'keyword'],
                            '@context' => ['type' => 'flattened']
                        ]
                    ]
                ]
            ]);
            $this->log("Created Elasticsearch index with mappings: {$this->esIndex}");
        }
    }

    public function getRecords(): array
    {
        $this->log("Fetching records from {$this->recordsApiUrl}", 'debug');
        try {
            $response = $this->httpClient->get($this->recordsApiUrl);
            $data = json_decode((string) $response->getBody(), true);
            
            $records = [];
            foreach ($data as $item) {
                if (isset($item['id'])) {
                    $records[$item['id']] = $item;
                }
            }

            return $records;
        } catch (\Exception $e) {
            $message = "Error fetching records API: " . $e->getMessage();
            $this->log($message, 'error');
            $this->crawlerErrorsCount++;
            $shortMessage = strlen($message) > 500 ? substr($message, 0, 500) . '...' : $message;
            $this->errorDetails[] = $shortMessage;
            return [];
        }
    }

    private function processDatasource(string $id, ?array $record = null): void
    {
        $this->processedInCurrentDatasource = 0;

        // Reset memory-intensive state for each datasource to prevent leaks and OOM on large crawls
        if (count($this->errorDetails) > $this->maxStoredErrors) {
            $this->errorDetails = array_slice($this->errorDetails, 0, $this->maxStoredErrors);
            $this->errorDetails[] = "... and more (too many unique errors logged in previous datasources)";
            $this->errorDetails = array_values(array_unique($this->errorDetails));
        }
        
        $this->currentDatasourceId = $id;
        
        if ($this->currentStat && $record && isset($record['ds_name_english'])) {
            $this->currentStat->addProcessedEntry((int)$id, $record['ds_name_english']);
        }
        
        $this->updateProgress();
        $this->pagesCrawledCount++;

        $archUrl = $record['odis_arch_url'] ?? null;
        $archType = strtolower($record['odis_arch_type'] ?? '');

        if (!$archUrl) {
            $url = $this->viewBaseUrl . $id;
            $this->log("Processing ID $id from $url (no pre-fetched record)", 'debug');

            try {
                $response = $this->httpClient->get($url);
                $html = (string) $response->getBody();
                $crawler = new Crawler($html);

                $crawler->filter('tr')->each(function (Crawler $node) use (&$archUrl, &$archType) {
                    $labelNode = $node->filter('td')->first();
                    if ($labelNode->count() > 0) {
                        $label = trim($labelNode->text());
                        if ($label === 'ODIS-Arch URL') {
                            $linkNode = $node->filter('td')->last()->filter('a');
                            if ($linkNode->count() > 0) {
                                $archUrl = trim($linkNode->attr('href'));
                            }
                        } elseif ($label === 'ODIS-Arch Type') {
                            $archType = strtolower(trim($node->filter('td')->last()->text()));
                        }
                    }
                });
            } catch (\Exception $e) {
                $this->log("Error processing ID $id: " . $e->getMessage(), 'error');
                $this->crawlerErrorsCount++;
                $this->errorDetails[] = "Error processing datasource $id: " . $e->getMessage();
                return;
            }
        } else {
            $this->log("Processing ID $id from pre-fetched record", 'debug');
        }

        if ($archUrl) {
            if (!$this->robotsManager->isAllowed($archUrl)) {
                $this->log("URL $archUrl is disallowed by robots.txt", 'warning');
                return;
            }
            $this->robotsManager->waitIfNecessary($archUrl);

            $this->log("Found $archType at $archUrl", 'debug');
            if ($archType === 'sitemap') {
                $this->processSitemap($archUrl);
            } elseif ($archType === 'sitegraph' || str_ends_with($archUrl, '.json')) {
                $this->fetchAndIndexJson($archUrl);
            } else {
                if (str_contains($archUrl, '.json')) {
                    $this->fetchAndIndexJson($archUrl);
                }
            }
        } else {
            $this->log("No ODIS-Arch URL found for ID $id", 'debug');
        }
    }

    private function processSitemap(string $sitemapUrl): void
    {
        if (!$this->robotsManager->isAllowed($sitemapUrl)) {
            $this->log("Sitemap URL $sitemapUrl is disallowed by robots.txt", 'warning');
            return;
        }
        $this->robotsManager->waitIfNecessary($sitemapUrl);

        $this->updateProgress();
        $this->pagesCrawledCount++;
        $this->log("Parsing sitemap: $sitemapUrl", 'debug');
        try {
            try {
                $response = $this->httpClient->get($sitemapUrl);
            } catch (\GuzzleHttp\Exception\ClientException $ce) {
                if ($ce->getResponse()->getStatusCode() === 404 && str_ends_with($sitemapUrl, '/assets/sitemap.xml')) {
                    $fallbackUrl = str_replace('/assets/sitemap.xml', '/sitemap.xml', $sitemapUrl);
                    $this->log("Sitemap 404 at $sitemapUrl. Attempting fallback: $fallbackUrl", 'warning');
                    $response = $this->httpClient->get($fallbackUrl);
                    $sitemapUrl = $fallbackUrl; // Update URL for logging/error reporting
                } else {
                    throw $ce;
                }
            }
            
            $xml = (string) $response->getBody();
            $contentType = $response->getHeaderLine('Content-Type');

            if (!str_contains($contentType, 'xml') && !str_starts_with(trim($xml), '<?xml') && !str_starts_with(trim($xml), '<sitemapindex') && !str_starts_with(trim($xml), '<urlset')) {
                $this->log("Sitemap URL $sitemapUrl returned non-XML content ($contentType). Treating as potential JSON-LD page.", 'warning');
                $this->fetchAndIndexJson($sitemapUrl);
                return;
            }

            try {
                $sitemap = new \SimpleXMLElement($xml);
            } catch (\Exception $e) {
                // If XML parsing fails, but it's not a clear 404, maybe it's a content page despite our checks
                $this->log("Failed to parse sitemap $sitemapUrl as XML. Attempting to treat as content page.", 'debug');
                $this->fetchAndIndexJson($sitemapUrl);
                return;
            }
            
            $namespaces = $sitemap->getNamespaces(true);
            $nsPrefix = '';
            if (isset($namespaces[''])) {
                $sitemap->registerXPathNamespace('s', $namespaces['']);
                $nsPrefix = 's:';
            }

            // Handle Sitemap Index
            if ($sitemap->getName() === 'sitemapindex') {
                $locs = $sitemap->xpath("//{$nsPrefix}loc");
                foreach ($locs as $loc) {
                    $this->processSitemap((string) $loc);
                }
                return;
            }

            // Use xpath to find all <loc> elements. We do it in chunks if possible but SimpleXML is not great for that.
            // For now, let's at least clear the SimpleXMLElement if it gets too large (not really possible)
            // But we can process the locs list more carefully
            $locs = $sitemap->xpath("//{$nsPrefix}loc");
            
            // Release XML string memory
            unset($xml);
            
            foreach ($locs as $loc) {
                if ($this->limit > 0 && $this->processedInCurrentDatasource >= $this->limit) {
                    $this->log("Limit of {$this->limit} reached for datasource {$this->currentDatasourceId}. Skipping remaining sitemap URLs.", 'info');
                    break;
                }
                $url = trim((string) $loc);
                if (empty($url)) continue;
                $this->fetchAndIndexJson($url);
                // Explicitly cleanup memory in sitemap loop
                gc_collect_cycles();
            }
            // Release sitemap object memory
            unset($sitemap);
            unset($locs);
            gc_collect_cycles();
        } catch (\Exception $e) {
            $message = "Error parsing sitemap $sitemapUrl: " . $e->getMessage();
            $this->log($message, 'error');
            $this->crawlerErrorsCount++;
            $shortMessage = strlen($message) > 500 ? substr($message, 0, 500) . '...' : $message;
            $this->errorDetails[] = $shortMessage;
        }
    }

    private function fetchAndIndexJson(string $url): void
    {
        if (!$this->robotsManager->isAllowed($url)) {
            $this->log("URL $url is disallowed by robots.txt", 'warning');
            return;
        }
        $this->robotsManager->waitIfNecessary($url);

        $this->updateProgress();
        $this->pagesCrawledCount++;
        $this->log("Fetching data from $url", 'debug');
        try {
            // Use streaming to avoid loading the entire body into memory if it's too large
            $response = $this->httpClient->get($url, ['stream' => true]);
            $bodyStream = $response->getBody();
            
            // If content length is known and > 50MB, we should be extra careful
            $contentLength = (int) $response->getHeaderLine('Content-Length');
            if ($contentLength > 50 * 1024 * 1024) {
                $this->log("Large response detected ($contentLength bytes). Processing with caution.", 'warning');
            }

            $contentType = $response->getHeaderLine('Content-Type');
            
            // To process the body we still need it as a string for json_decode or extractJsonLdFromHtml
            // but we can try to minimize memory spikes
            $body = $bodyStream->getContents();
            
            $data = null;
            if (str_contains($contentType, 'application/json') || str_contains($contentType, 'application/ld+json')) {
                $data = json_decode($body, true);
            } elseif (str_contains($contentType, 'text/html')) {
                $data = $this->extractJsonLdFromHtml($body);
            } else {
                // Fallback: try JSON first, then HTML if it looks like it might have JSON-LD
                $data = json_decode($body, true);
                if (!$data) {
                    $data = $this->extractJsonLdFromHtml($body);
                }
            }
            
            // Free body string memory as soon as possible
            unset($body);

            if ($data) {
                // Handle both single object and list of objects
                $items = isset($data[0]) ? $data : [$data];
                
                foreach ($items as $item) {
                    if (empty($item) || !is_array($item)) continue;
                    
                    if ($this->limit > 0 && $this->processedInCurrentDatasource >= $this->limit) {
                        break;
                    }

                    // Sanitize polymorphic fields that can be mixed string/object
                    $polymorphicFields = ['knowsAbout', 'keywords', 'contributor', 'distribution', 'identifier'];
                    foreach ($polymorphicFields as $field) {
                        if (isset($item[$field])) {
                            if (is_string($item[$field])) {
                                $item[$field] = ['name' => $item[$field]];
                            } elseif (is_array($item[$field])) {
                                // If it's an array, ensure all elements are objects
                                $newValues = [];
                                foreach ($item[$field] as $val) {
                                    if (is_string($val)) {
                                        $newValues[] = ['name' => $val];
                                    } else {
                                        $newValues[] = $val;
                                    }
                                }
                                $item[$field] = $newValues;
                            }
                        }
                    }
                    
                    // Ensure a URL field exists if not present in the metadata
                    if (!isset($item['url'])) {
                        $item['url'] = $url;
                    }
                    
                    // Exclude massive fields that aren't useful for search/discovery
                    // to keep the index size manageable and prevent OOM during search
                    $largeFieldsToExclude = ['text', 'description', 'keywords', 'subjectOf', 'about', 'funder', 'spatialCoverage', 'geo', 'potentialAction', 'mentions', 'hasCourseInstance'];
                    foreach ($largeFieldsToExclude as $field) {
                        if (isset($item[$field]) && is_string($item[$field]) && strlen($item[$field]) > 10000) {
                            $item[$field] = substr($item[$field], 0, 10000) . '... (truncated for index efficiency)';
                        }
                    }
                    
                    // Link to the ODISCat datasource if we have it
                    if ($this->currentDatasourceId && !isset($item['odis_cat_id'])) {
                        $item['odis_cat_id'] = $this->currentDatasourceId;
                        $item['odis_cat_url'] = "https://catalogue.odis.org/view/" . $this->currentDatasourceId;
                    }
                    
                    // Use @id from data if available, otherwise use crawl URL
                    $docId = isset($item['@id']) && is_string($item['@id']) ? $item['@id'] : $url;

                    // Ensure we always have the original crawl URL for fallback
                    $item['_crawl_url'] = $url;
                    
                    // Normalize docId to prevent duplicates from minor string variations
                    $docId = trim($docId);
                    
                    // Aggressive URL normalization for deduplication
                    if (filter_var($docId, FILTER_VALIDATE_URL)) {
                        $docId = rtrim(strtolower($docId), '/');
                    }
                    
                    // Use MD5 hash of the normalized ID for Elasticsearch to ensure consistent and valid IDs
                    $idHash = md5($docId);

                    $params = [
                        'index' => $this->esIndex,
                        'id'    => $idHash,
                        'body'  => $item
                    ];
                    
            try {
                $this->esClient->index($params);
                $this->log("Indexed metadata from $url (ID: $docId, Hash: $idHash)", 'debug');
                $this->validJsonLdsCount++;
                $this->processedInCurrentDatasource++;
                if ($this->currentStat) {
                    $this->currentStat->incrementEntryRecordsFound();
                    $this->currentStat->incrementEntryValidJsonLds();
                }
            } catch (\Exception $indexEx) {
                $message = $indexEx->getMessage();
                $this->log("Failed to index item from $url: " . $message, 'error');
                $this->invalidJsonLdsCount++;
                $shortMessage = strlen($message) > 500 ? substr($message, 0, 500) . '...' : $message;
                $this->errorDetails[] = "Invalid JSON-LD format at $url: $shortMessage";
                if ($this->currentStat) {
                    $this->currentStat->incrementEntryRecordsFound();
                    $this->currentStat->incrementEntryErrorsCount();
                    $this->currentStat->addEntryError("Indexing error: " . $shortMessage);
                }
            }
                }
                
                // Cleanup items and data
                unset($items);
                unset($data);
                gc_collect_cycles();
            } else {
                $this->log("No JSON-LD found at $url", 'warning');
                $this->invalidJsonLdsCount++;
                $this->errorDetails[] = "No JSON-LD found at $url";
                if ($this->currentStat) {
                    $this->currentStat->incrementEntryErrorsCount();
                    $this->currentStat->addEntryError("No JSON-LD found at $url");
                }
            }
        } catch (\Exception $e) {
            $message = $e->getMessage();
            $this->log("Error fetching/indexing data from $url: " . $message, 'error');
            $this->invalidJsonLdsCount++;
            
            // Categorize errors
            if (str_contains($message, '400 Bad Request') || str_contains($message, 'document_parsing_exception')) {
                // This is a data/mapping error (Invalid Format)
                $shortMessage = strlen($message) > 500 ? substr($message, 0, 500) . '...' : $message;
                $this->errorDetails[] = "Invalid JSON-LD format at $url: $shortMessage";
                if ($this->currentStat) {
                    $this->currentStat->incrementEntryErrorsCount();
                    $this->currentStat->addEntryError("Format error: " . $shortMessage);
                }
            } else {
                // This is a network or other system error
                $this->crawlerErrorsCount++;
                $shortMessage = strlen($message) > 500 ? substr($message, 0, 500) . '...' : $message;
                $this->errorDetails[] = "Error fetching data from $url: $shortMessage";
                if ($this->currentStat) {
                    $this->currentStat->incrementEntryErrorsCount();
                    $this->currentStat->addEntryError("Fetch error: " . $shortMessage);
                }
            }
        }
    }

    private function extractJsonLdFromHtml(string $html): ?array
    {
        try {
            // To avoid memory issues with large HTML files, if the file is truly massive,
            // we should try to extract the JSON-LD without building a full DOM if possible.
            // But for now, let's at least try to be efficient.
            $crawler = new Crawler($html);
            $jsonLdScripts = $crawler->filter('script[type="application/ld+json"]');
            
            $results = [];
            if ($jsonLdScripts->count() > 0) {
                $jsonLdScripts->each(function (Crawler $node) use (&$results) {
                    $json = trim($node->text());
                    if (empty($json)) return;
                    
                    $decoded = json_decode($json, true);
                    unset($json); // Clear large string
                    
                    if ($decoded) {
                        if (isset($decoded['@graph']) && is_array($decoded['@graph'])) {
                            foreach ($decoded['@graph'] as $item) {
                                $results[] = $item;
                            }
                        } else {
                            $results[] = $decoded;
                        }
                    }
                    unset($decoded);
                });
            }
            
            // Cleanup the crawler object early
            unset($crawler);
            gc_collect_cycles();
            
            return !empty($results) ? $results : null;
        } catch (\Exception $e) {
            $this->log("Error extracting JSON-LD from HTML: " . $e->getMessage(), 'warning');
        }
        
        return null;
    }
}
