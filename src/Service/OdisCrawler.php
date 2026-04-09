<?php

namespace App\Service;

use Elastic\Elasticsearch\ClientInterface;
use Elastic\Transport\Exception\NoNodeAvailableException;
use GuzzleHttp\Client as GuzzleClient;
use Symfony\Component\DomCrawler\Crawler;
use Psr\Log\LoggerInterface;
use App\Entity\CrawlStat;
use Doctrine\ORM\EntityManagerInterface;

use Symfony\Component\Console\Output\OutputInterface;

class OdisCrawler
{
    private GuzzleClient $httpClient;
    private ClientInterface $esClient;
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
    private array $visitedSitemaps = [];

    public function __construct(ClientInterface $esClient, LoggerInterface $logger, EntityManagerInterface $entityManager, RobotsTxtManager $robotsManager, ?GuzzleClient $httpClient = null)
    {
        $this->esClient = $esClient;
        $this->logger = $logger;
        $this->entityManager = $entityManager;
        $this->robotsManager = $robotsManager;
        $this->httpClient = $httpClient ?: new GuzzleClient([
            'timeout'  => 15.0,
            'verify' => false,
            'headers' => [
                'Accept' => 'text/html,application/json,application/ld+json;q=0.9,*/*;q=0.8',
                'User-Agent' => 'ODIS https://search.odis.org',
            ]
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
        $this->visitedSitemaps = [];
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
            $this->errorDetails[] = [
                'id' => null,
                'message' => $shortMessage
            ];
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

        if ($this->limit > 0 && empty($specificIds)) {
            $this->log("Applying limit of {$this->limit} datasources");
            $dsIds = array_slice($dsIds, 0, $this->limit);
            $this->nodesFoundCount = count($dsIds);
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
        $temp = [];
        foreach ($this->errorDetails as $error) {
            $key = is_array($error) ? json_encode($error) : $error;
            $temp[$key] = $error;
        }
        $this->errorDetails = array_values($temp);

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
            $temp = [];
            foreach ($this->errorDetails as $error) {
                $key = is_array($error) ? json_encode($error) : $error;
                $temp[$key] = $error;
            }
            $this->errorDetails = array_values($temp);
            
            if (count($this->errorDetails) > $this->maxStoredErrors) {
                $this->errorDetails = array_slice($this->errorDetails, 0, $this->maxStoredErrors);
                $this->errorDetails[] = [
                    'id' => null,
                    'message' => "... and more (too many unique errors logged)"
                ];
                // Final re-deduplicate to avoid duplicate "... and more"
                $temp = [];
                foreach ($this->errorDetails as $error) {
                    $key = is_array($error) ? json_encode($error) : $error;
                    $temp[$key] = $error;
                }
                $this->errorDetails = array_values($temp);
            }

            $this->saveStats('in_progress');
            $this->lastUpdateTimestamp = time();
        }
    }

    public function clearIndex(): void
    {
        $params = ['index' => $this->esIndex];
        try {
            if ($this->esClient->indices()->exists($params)->asBool()) {
                $this->esClient->indices()->delete($params);
                $this->log("Deleted Elasticsearch index: {$this->esIndex}");
                
                // Wait a bit for the index to be fully deleted before allowing creation
                // This helps avoid resource_already_exists_exception on fast sequential calls
                usleep(500000); // 500ms
            }
        } catch (NoNodeAvailableException $e) {
            $message = "Elasticsearch connection failed: " . $e->getMessage() . ". Solution: Please check your ELASTICSEARCH_URL in .env.local and ensure the Elasticsearch service is running.";
            $this->log($message, 'error');
            throw new \RuntimeException($message, 0, $e);
        } catch (\Exception $e) {
            $message = "Failed to clear Elasticsearch index: " . $e->getMessage();
            $this->log($message, 'error');
            throw new \RuntimeException($message, 0, $e);
        }
    }

    public function getIndexMapping(): array
    {
        return [
            'dynamic_templates' => [
                [
                    'data_fields' => [
                        'path_match' => 'data.*',
                        'mapping' => [ 
                            'type' => 'object',
                            'enabled' => false
                        ]
                    ]
                ]
            ],
            'properties' => [
                'name' => [
                    'type' => 'text',
                    'fields' => ['keyword' => ['type' => 'keyword']]
                ],
                'description' => ['type' => 'text'],
                'keywords' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'schema:keywords' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'knowsAbout' => ['type' => 'flattened'],
                'distribution' => ['type' => 'flattened'],
                'identifier' => ['type' => 'flattened'],
                'creator' => ['type' => 'flattened'],
                'provider' => ['type' => 'flattened'],
                'schema:provider' => ['type' => 'flattened'],
                'funder' => ['type' => 'flattened'],
                'schema:funder' => ['type' => 'flattened'],
                'publisher' => ['type' => 'flattened'],
                'schema:publisher' => ['type' => 'flattened'],
                'author' => ['type' => 'flattened'],
                'schema:author' => ['type' => 'flattened'],
                'contributor' => ['type' => 'flattened'],
                'schema:contributor' => ['type' => 'flattened'],
                'about' => ['type' => 'flattened'],
                'mentions' => ['type' => 'flattened'],
                'subjectOf' => ['type' => 'flattened'],
                'spatialCoverage' => ['type' => 'flattened'],
                'temporalCoverage' => ['type' => 'flattened'],
                'geo' => ['type' => 'flattened'],
                'schema:creator' => ['type' => 'flattened'],
                'schema:about' => ['type' => 'flattened'],
                'schema:mentions' => ['type' => 'flattened'],
                'schema:subjectOf' => ['type' => 'flattened'],
                'schema:spatialCoverage' => ['type' => 'flattened'],
                'schema:temporalCoverage' => ['type' => 'flattened'],
                'schema:geo' => ['type' => 'flattened'],
                'schema:distribution' => ['type' => 'flattened'],
                'schema:identifier' => ['type' => 'flattened'],
                'potentialAction' => ['type' => 'flattened'],
                'schema:potentialAction' => ['type' => 'flattened'],
                'hasCourseInstance' => ['type' => 'flattened'],
                'schema:hasCourseInstance' => ['type' => 'flattened'],
                'sameAs' => ['type' => 'flattened'],
                'schema:sameAs' => ['type' => 'flattened'],
                'variableMeasured' => ['type' => 'flattened'],
                'schema:variableMeasured' => ['type' => 'flattened'],
                'includedInDataCatalog' => ['type' => 'flattened'],
                'schema:includedInDataCatalog' => ['type' => 'flattened'],
                '@type' => [
                    'type' => 'text',
                    'fields' => ['keyword' => ['type' => 'keyword']]
                ],
                'url' => ['type' => 'keyword'],
                'schema:name' => [
                    'type' => 'text',
                    'fields' => ['keyword' => ['type' => 'keyword']]
                ],
                'schema:description' => ['type' => 'text'],
                'license' => ['type' => 'flattened'],
                'schema:license' => ['type' => 'flattened'],
                'citation' => ['type' => 'flattened'],
                'schema:citation' => ['type' => 'flattened'],
                'version' => ['type' => 'flattened'],
                'schema:version' => ['type' => 'flattened'],
                'encodingFormat' => ['type' => 'flattened'],
                'schema:encodingFormat' => ['type' => 'flattened'],
                'startDate' => ['type' => 'flattened'],
                'schema:startDate' => ['type' => 'flattened'],
                'endDate' => ['type' => 'flattened'],
                'schema:endDate' => ['type' => 'flattened'],
                'location' => ['type' => 'flattened'],
                'schema:location' => ['type' => 'flattened'],
                'arrivalBoatTerminal' => ['type' => 'flattened'],
                'schema:arrivalBoatTerminal' => ['type' => 'flattened'],
                'departureBoatTerminal' => ['type' => 'flattened'],
                'schema:departureBoatTerminal' => ['type' => 'flattened'],
                'subEvent' => ['type' => 'flattened'],
                'schema:subEvent' => ['type' => 'flattened'],
                'sdPublisher' => ['type' => 'flattened'],
                'schema:sdPublisher' => ['type' => 'flattened'],
                'datePublished' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'schema:datePublished' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'educationalCredentialAwarded' => ['type' => 'flattened'],
                'schema:educationalCredentialAwarded' => ['type' => 'flattened'],
                'contactPoint' => ['type' => 'flattened'],
                'schema:contactPoint' => ['type' => 'flattened'],
                'inLanguage' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'schema:inLanguage' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'data' => [
                    'type' => 'object',
                    'dynamic' => true
                ],
                '@context' => ['type' => 'flattened']
            ]
        ];
    }

    public function createIndex(): void
    {
        $this->esClient->indices()->create([
            'index' => $this->esIndex,
            'body' => [
                'mappings' => $this->getIndexMapping()
            ]
        ]);
        $this->log("Created Elasticsearch index: {$this->esIndex}");
    }

    private function ensureIndexExists(): void
    {
        $params = ['index' => $this->esIndex];
        if (!$this->esClient->indices()->exists($params)->asBool()) {
            $this->createIndex();
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
            $this->errorDetails[] = [
                'id' => null,
                'message' => $shortMessage
            ];
            return [];
        }
    }

    private function processDatasource(string $id, ?array $record = null): void
    {
        $this->processedInCurrentDatasource = 0;

        // Reset memory-intensive state for each datasource to prevent leaks and OOM on large crawls
        if (count($this->errorDetails) > $this->maxStoredErrors) {
            $this->errorDetails = array_slice($this->errorDetails, 0, $this->maxStoredErrors);
            $this->errorDetails[] = [
                'id' => null,
                'message' => "... and more (too many unique errors logged in previous datasources)"
            ];
            // Deduplicate
            $temp = [];
            foreach ($this->errorDetails as $error) {
                $key = is_array($error) ? json_encode($error) : $error;
                $temp[$key] = $error;
            }
            $this->errorDetails = array_values($temp);
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
        if (in_array($sitemapUrl, $this->visitedSitemaps)) {
            $this->log("Sitemap already visited: $sitemapUrl. Skipping to prevent infinite loop.", 'warning');
            return;
        }
        $this->visitedSitemaps[] = $sitemapUrl;

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
                    if ($this->limit > 0 && $this->processedInCurrentDatasource >= $this->limit) {
                        break;
                    }
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
            $this->errorDetails[] = [
                'id' => $this->currentDatasourceId,
                'message' => $shortMessage
            ];
        }
    }

    public function normalizeDataForSafeIndexing(array $data): array
    {
        foreach ($data as $key => $value) {
            if (is_array($value)) {
                // Check if it's an associative array (object in JSON)
                $isAssoc = false;
                if (!empty($value)) {
                    $keys = array_keys($value);
                    $isAssoc = array_keys($keys) !== $keys;
                }

                if ($isAssoc) {
                    // It's an object, we recurse into it
                    $data[$key] = $this->normalizeDataForSafeIndexing($value);
                } else {
                    // It's an indexed array. 
                }
            } else {
                // Scalar value (string, number, bool, null).
                // Since data.* is now mapped as 'type: object, enabled: false' via dynamic template,
                // Elasticsearch EXPECTS an object for EVERY field under data.
                // If we send a scalar, it will fail.
                // Solution: Wrap scalars in a simple object.
                $data[$key] = ['value' => $value];
            }
        }
        return $data;
    }

    private function normalizePolymorphicField($value): ?array
    {
        if ($value === null) {
            return null;
        }

        if (is_string($value)) {
            return [['name' => $value]];
        }

        if (is_array($value)) {
            // Check if it's an associative array (single object)
            $isAssoc = false;
            if (!empty($value)) {
                $keys = array_keys($value);
                $isAssoc = array_keys($keys) !== $keys;
            }

            if ($isAssoc) {
                // It's a single object, wrap it in an array to be consistent
                return [$value];
            } else {
                // It's an indexed array, ensure all elements are objects
                $newValues = [];
                foreach ($value as $val) {
                    if ($val === null) {
                        continue;
                    }
                    if (is_string($val)) {
                        $newValues[] = ['name' => $val];
                    } elseif (is_array($val)) {
                        $newValues[] = $val;
                    } else {
                        // scalar or other, convert to string
                        $newValues[] = ['name' => (string)$val];
                    }
                }
                return $newValues;
            }
        }

        // scalar but not string, convert to string wrapped in object
        return [['name' => (string)$value]];
    }

    private function getSolutionForError(string $message): string
    {
        if (str_contains($message, 'No alive nodes') || str_contains($message, 'NoNodeAvailableException')) {
            return "Elasticsearch is unreachable. Solution: Check your ELASTICSEARCH_URL in .env.local and ensure the Elasticsearch service is running and accessible from the server.";
        }
        
        if (str_contains($message, '401 Unauthorized') || str_contains($message, '403 Forbidden')) {
            return "Elasticsearch authentication failed. Solution: Check ELASTICSEARCH_USER and ELASTICSEARCH_PASSWORD in your .env.local.";
        }

        if (str_contains($message, '400 Bad Request') || str_contains($message, 'document_parsing_exception') || str_contains($message, 'illegal_argument_exception')) {
            if (str_contains($message, 'failed to parse field')) {
                preg_match('/failed to parse field \[([^\]]+)\] of type \[([^\]]+)\]/', $message, $matches);
                $field = $matches[1] ?? 'unknown';
                $type = $matches[2] ?? 'unknown';
                
                $solution = "CRITICAL MAPPING CONFLICT: The field '$field' is currently mapped as '$type' in Elasticsearch, but the crawler is trying to send it as a different structure. ";
                $solution .= "To fix this, you MUST reset your index by running: php bin/console app:odis:crawl --clear-index";
                
                if ($type === 'date') {
                    $solution .= " (Date fields are particularly sensitive to structure changes).";
                }
                
                return $solution;
            }
            return "Elasticsearch indexing failed due to a mapping conflict or invalid document structure. Solution: You MUST run the crawl with the '--clear-index' flag to reset the index and apply new mappings.";
        }
        
        if (str_contains($message, 'Syntax error') || str_contains($message, 'Control character error')) {
            return "The JSON-LD contains syntax errors (e.g. missing commas, unescaped quotes). Solution: Use a JSON validator or the JSON-LD Playground (https://json-ld.org/playground/) to fix the source data.";
        }

        if (str_contains($message, 'No JSON-LD found')) {
            return "No <script type=\"application/ld+json\"> tags were found on the page. Solution: Ensure the page contains valid JSON-LD metadata.";
        }

        if (str_contains($message, '500 Internal Server Error') || str_contains($message, '404 Not Found')) {
            return "The server returned a terminal error. Solution: Check if the URL is accessible and the server is healthy.";
        }

        return "Unknown error occurred during processing. Solution: Check the crawler logs for more details.";
    }

    public function fetchAndIndexJson(string $url): void
    {
        if (!$this->robotsManager->isAllowed($url)) {
            $this->log("URL $url is disallowed by robots.txt", 'warning');
            return;
        }
        $this->robotsManager->waitIfNecessary($url);

        $this->updateProgress();
        $this->pagesCrawledCount++;
        $this->processedInCurrentDatasource++;
        if ($this->currentStat) {
            $this->currentStat->incrementEntryRecordsFound();
        }
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
            $this->log("Processing body with content type: $contentType", 'debug');
            if (str_contains($contentType, 'application/json') || str_contains($contentType, 'application/ld+json')) {
                // If it's JSON, it might still have leading/trailing whitespace or UTF-8 BOM
                $body = trim($body);
                // Remove UTF-8 BOM if present
                if (str_starts_with($body, "\xEF\xBB\xBF")) {
                    $body = substr($body, 3);
                }
                
                $data = json_decode($body, true);
                if ($data === null) {
                    $jsonError = json_last_error_msg();
                    $shortBody = substr($body, 0, 200);
                    $fullError = "JSON decoding failed for $url: $jsonError. Body starts with: $shortBody";
                    $solution = $this->getSolutionForError($jsonError);
                    
                    $this->log($fullError, 'warning');
                    
                    // Fallback to HTML extraction if JSON decode failed but it might be HTML mislabeled as JSON
                    if (str_contains($body, '<html')) {
                        $this->log("Content contains <html> tag, attempting HTML extraction for $url", 'debug');
                        $data = $this->extractJsonLdFromHtml($body);
                    }
                    
                    if (!$data) {
                        $this->invalidJsonLdsCount++;
                        $errorMsg = "Invalid JSON: $jsonError. $solution";
                        $this->errorDetails[] = [
                            'id' => $this->currentDatasourceId,
                            'message' => $errorMsg,
                            'url' => $url
                        ];
                        if ($this->currentStat) {
                            $this->currentStat->incrementEntryErrorsCount();
                            $this->currentStat->addEntryError($errorMsg);
                            $this->currentStat->addErrorDetail([
                                'id' => $this->currentDatasourceId,
                                'message' => $errorMsg,
                                'url' => $url
                            ]);
                        }
                    }
                }
            } elseif (str_contains($contentType, 'text/html')) {
                $data = $this->extractJsonLdFromHtml($body);
            } else {
                // Fallback: try JSON first, then HTML if it looks like it might have JSON-LD
                $data = json_decode($body, true);
                if (!$data) {
                    $data = $this->extractJsonLdFromHtml($body);
                }
                unset($body); // Free memory immediately
            }
            
            if ($data) {
                // Treat as a sitegraph in the following cases:
                // - JSON-LD object with an '@graph' array (any size, including 1)
                // - Top-level JSON array (list) of items
            $isTopLevelList = is_array($data) && array_is_list($data);
            $isGraph = (isset($data['@graph']) && is_array($data['@graph'])) || 
                       (isset($data['itemListElement']) && is_array($data['itemListElement'])) || 
                       (isset($data['dataset']) && is_array($data['dataset'])) ||
                       $isTopLevelList;
            
            // Special case for ODIS sitegraphs that might be wrapped in @graph but have other keys
            // or where @graph is not at the top level, or it's a list with different keys.
            if (!$isGraph && isset($data['graph']) && is_array($data['graph'])) {
                $isGraph = true;
                $graph = $data['graph'];
            } elseif (!$isGraph && str_ends_with($url, '.json') && count($data) > 0 && !isset($data['@type'])) {
                // If it's a large associative array with many numeric keys or just many keys
                // and no @type, it's likely a collection.
                $isGraph = true;
                $graph = $data;
            } elseif ($isGraph) {
                if ($isTopLevelList) {
                    $graph = $data;
                } elseif (isset($data['@graph']) && is_array($data['@graph'])) {
                    $graph = $data['@graph'];
                } elseif (isset($data['itemListElement']) && is_array($data['itemListElement'])) {
                    $graph = $data['itemListElement'];
                } elseif (isset($data['dataset']) && is_array($data['dataset'])) {
                    $graph = $data['dataset'];
                }
            }

                if ($isGraph) {
                    $this->log("Graph detected at $url (" . count($graph) . " items). Indexing individually.", 'info');
                    unset($data); // Free parent immediately
                    foreach ($graph as $index => $item) {
                        if ($this->limit > 0 && $this->validJsonLdsCount >= $this->limit) {
                            break;
                        }
                        
                        // If item is not an array, skip it
                        if (!is_array($item)) {
                            continue;
                        }

                        // Normalize data for safe indexing
                        $item = $this->normalizeDataForSafeIndexing($item);

                        // Extract root-level fields from wrapped objects before indexing
                        $rootFields = [
                            'name', 'schema:name', 'title', 'schema:title',
                            'description', 'schema:description', 
                            '@type', 'schema:@type', 'keywords', 'schema:keywords',
                            'inLanguage', 'schema:inLanguage', 'datePublished', 'schema:datePublished'
                        ];

                        $type = $item['@type']['value'] ?? $item['@type'] ?? '';

                        // Skip BreadcrumbList as it's not a valid ODIS type for indexing
                        if ($type === 'BreadcrumbList' || $type === 'schema:BreadcrumbList') {
                            $this->log("Skipping BreadcrumbList at $url", 'debug');
                            continue;
                        }

                        // Handle ListItem: extract the actual 'item' content if present
                        if ($type === 'ListItem' || $type === 'schema:ListItem') {
                            if (isset($item['item']) && is_array($item['item'])) {
                                $item = $item['item'];
                                $type = $item['@type']['value'] ?? $item['@type'] ?? '';
                            }
                        }

                        // Re-check type after possible ListItem unwrap
                        if ($type === 'BreadcrumbList' || $type === 'schema:BreadcrumbList') {
                            $this->log("Skipping BreadcrumbList (unwrapped) at $url", 'debug');
                            continue;
                        }

                        $itemId = $item['@id'] ?? $item['id'] ?? null;
                        if (is_array($itemId)) {
                            $itemId = json_encode($itemId);
                        }
                        $itemId = $itemId ?: md5($url . $index);

                    $body = [
                        'url' => $url,
                        'data' => $item,
                        'datasource_id' => $this->currentDatasourceId,
                        'indexed_at' => (new \DateTime())->format('Y-m-d H:i:s')
                    ];

                        foreach ($rootFields as $field) {
                            if (isset($item[$field])) {
                                if (is_array($item[$field]) && isset($item[$field]['value'])) {
                                    $val = $item[$field]['value'];
                                } else {
                                    $val = $item[$field];
                                }

                                if (($field === 'keywords' || $field === 'schema:keywords') && is_array($val)) {
                                    $val = implode(', ', array_map(function($k) {
                                        return is_array($k) ? ($k['value'] ?? json_encode($k)) : $k;
                                    }, $val));
                                }
                            
                                // If title was found, map it to name for consistent indexing
                                if ($field === 'title' || $field === 'schema:title') {
                                    $body['name'] = $val;
                                }
                            
                                $body[$field] = $val;
                            }
                        }

                        $params = [
                            'index' => 'odis_metadata',
                            'id'    => md5($itemId),
                            'body'  => $body
                        ];
                        try {
                            $this->esClient->index($params);
                            $this->validJsonLdsCount++;
                        } catch (\Exception $e) {
                            $this->log("Failed to index item from $url: " . $e->getMessage(), 'error');
                        }
                        unset($graph[$index]); // Free item after indexing
                    }
                    unset($graph);
                } else {
                    $params = [
                        'index' => 'odis_metadata',
                        'id'    => md5($url),
                        'body'  => [
                            'url' => $url,
                            'datasource_id' => $this->currentDatasourceId,
                            'indexed_at' => (new \DateTime())->format('Y-m-d H:i:s')
                        ]
                    ];

                    // Normalize data for safe indexing
                    $normalizedData = $this->normalizeDataForSafeIndexing($data);
                    
                    $type = $normalizedData['@type']['value'] ?? $normalizedData['@type'] ?? '';

                    // Skip BreadcrumbList
                    if ($type === 'BreadcrumbList' || $type === 'schema:BreadcrumbList') {
                        $this->log("Skipping BreadcrumbList at $url", 'debug');
                        return;
                    }

                    // Special case for ItemList and ListItem: unwrap if they contain an 'item'
                    if (($type === 'ListItem' || $type === 'schema:ListItem') && isset($normalizedData['item'])) {
                        $innerItem = $normalizedData['item'];
                        foreach ($normalizedData as $k => $v) {
                            if ($k !== 'item' && !isset($innerItem[$k])) {
                                $innerItem[$k] = $v;
                            }
                        }
                        $normalizedData = $innerItem;
                        $type = $normalizedData['@type']['value'] ?? $normalizedData['@type'] ?? '';
                    }

                    // Re-check type after possible ListItem unwrap
                    if ($type === 'BreadcrumbList' || $type === 'schema:BreadcrumbList') {
                        $this->log("Skipping BreadcrumbList (unwrapped) at $url", 'debug');
                        return;
                    }

                    $params['body']['data'] = $normalizedData;

                    // Extract root-level fields from wrapped objects before indexing
                    $rootFields = [
                        'name', 'schema:name', 'title', 'schema:title',
                        'description', 'schema:description', 
                        '@type', 'schema:@type', 'keywords', 'schema:keywords',
                        'inLanguage', 'schema:inLanguage', 'datePublished', 'schema:datePublished'
                    ];
                    foreach ($rootFields as $field) {
                        if (isset($normalizedData[$field])) {
                            if (is_array($normalizedData[$field]) && isset($normalizedData[$field]['value'])) {
                                $val = $normalizedData[$field]['value'];
                            } else {
                                $val = $normalizedData[$field];
                            }

                            if (($field === 'keywords' || $field === 'schema:keywords') && is_array($val)) {
                                $val = implode(', ', array_map(function($k) {
                                    return is_array($k) ? ($k['value'] ?? json_encode($k)) : $k;
                                }, $val));
                            }
                            
                            // If title was found, map it to name for consistent indexing
                            if ($field === 'title' || $field === 'schema:title') {
                                $params['body']['name'] = $val;
                            }
                            
                            $params['body'][$field] = $val;
                        }
                    }
                    unset($data); // Free memory before indexing
                    unset($normalizedData);
                    
                    try {
                        $this->esClient->index($params);
                        $this->validJsonLdsCount++;
                    } catch (\Exception $e) {
                        $this->log("Failed to index item from $url: " . $e->getMessage(), 'error');
                    }
                }
            } else {
                $this->log("No JSON-LD found at $url. Content-Type: $contentType. Body starts with: " . substr($body, 0, 100), 'warning');
                $this->invalidJsonLdsCount++;
                $this->errorDetails[] = [
                    'id' => $this->currentDatasourceId,
                    'message' => "No JSON-LD found at $url (Content-Type: $contentType)"
                ];
                if ($this->currentStat) {
                    $this->currentStat->incrementEntryErrorsCount();
                    $this->currentStat->addEntryError("No JSON-LD found at $url");
                    $this->currentStat->addErrorDetail([
                        'id' => $this->currentDatasourceId,
                        'message' => "No JSON-LD found at $url",
                        'url' => $url
                    ]);
                }
            }
            
            // Free body string memory as soon as possible
            unset($body);
        } catch (\Exception $e) {
            $message = $e->getMessage();
            $this->log("Error fetching/indexing data from $url: " . $message, 'error');
            $this->invalidJsonLdsCount++;
            
            $solution = $this->getSolutionForError($message);
            $shortMessage = strlen($message) > 500 ? substr($message, 0, 500) . '...' : $message;
            $fullMessageWithSolution = "$shortMessage. $solution";
            
            // Categorize errors
            if (str_contains($message, '400 Bad Request') || str_contains($message, 'document_parsing_exception')) {
                // This is a data/mapping error (Invalid Format)
                $this->errorDetails[] = [
                    'id' => $this->currentDatasourceId,
                    'message' => "Invalid JSON-LD format at $url: $fullMessageWithSolution"
                ];
                if ($this->currentStat) {
                    $this->currentStat->incrementEntryErrorsCount();
                    $this->currentStat->addEntryError("Format error: " . $fullMessageWithSolution);
                    $this->currentStat->addErrorDetail([
                        'id' => $this->currentDatasourceId,
                        'message' => "Invalid JSON-LD format at $url: $fullMessageWithSolution"
                    ]);
                }
            } else {
                // This is a network or other system error
                $this->crawlerErrorsCount++;
                $this->errorDetails[] = [
                    'id' => $this->currentDatasourceId,
                    'message' => "Error fetching data from $url: $fullMessageWithSolution"
                ];
                if ($this->currentStat) {
                    $this->currentStat->incrementEntryErrorsCount();
                    $this->currentStat->addEntryError("Fetch error: " . $fullMessageWithSolution);
                    $this->currentStat->addErrorDetail([
                        'id' => $this->currentDatasourceId,
                        'message' => "Error fetching data from $url: $fullMessageWithSolution"
                    ]);
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
                    $json = trim($node->getNode(0)->textContent);
                    if (empty($json)) return;
                    
                    $decoded = json_decode($json, true);
                    if ($decoded === null && !empty($json)) {
                        // Fallback: try decoding HTML entities and removing control characters
                        $cleaned = html_entity_decode(preg_replace('/[\x00-\x1F\x7F]/', ' ', $json));
                        $decoded = json_decode($cleaned, true);
                        
                        if ($decoded === null) {
                            // Aggressive fallback: handle unescaped internal double quotes in some common GeoNetwork patterns
                            // e.g. "alternateTitle": "Gas pipelines ... "as built" in ..."
                            // We look for " followed by characters then " then characters then " where it's part of a value
                            // This is risky but helps with broken source data
                            $cleanedAggressive = preg_replace_callback('/(": ")(.*?)("(?:\s*)[,}\]])/s', function($matches) {
                                $value = $matches[2];
                                // Escape internal quotes in the value
                                $value = str_replace('"', '\"', $value);
                                return $matches[1] . $value . $matches[3];
                            }, $cleaned);
                            $decoded = json_decode($cleanedAggressive, true);
                        }
                    }

                    if ($decoded === null && !empty($json)) {
                        $jsonError = json_last_error_msg();
                        $solution = $this->getSolutionForError($jsonError);
                        $this->log("Failed to decode JSON-LD snippet: $jsonError. $solution (first 100 chars: " . substr($json, 0, 100) . ")", 'warning');
                    }
                    unset($json); // Clear large string
                    
                    if ($decoded) {
                        if (isset($decoded['@graph']) && is_array($decoded['@graph'])) {
                            foreach ($decoded['@graph'] as $item) {
                                $results[] = $item;
                            }
                        } elseif (isset($decoded['@type']) && (is_string($decoded['@type']) ? $decoded['@type'] === 'ItemList' : in_array('ItemList', $decoded['@type'])) && isset($decoded['itemListElement'])) {
                            foreach ($decoded['itemListElement'] as $element) {
                                if (isset($element['item'])) {
                                    $results[] = $element['item'];
                                } else {
                                    $results[] = $element;
                                }
                            }
                        } elseif (isset($decoded['hasPart'])) {
                            $parts = is_array($decoded['hasPart']) ? $decoded['hasPart'] : [$decoded['hasPart']];
                            foreach ($parts as $part) {
                                $results[] = $part;
                            }
                        } elseif (is_array($decoded) && array_is_list($decoded)) {
                            foreach ($decoded as $item) {
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
