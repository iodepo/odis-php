<?php
/**
 * This file is part of the ODIS PHP project.
 *
 * @category Service
 * @package  App\Service
 * @author   Arno Lambert <a.lambert@unesco.org>
 * @author  Junie Pro
 */

namespace App\Service;

use Elastic\Elasticsearch\ClientInterface;
use Elastic\Transport\Exception\NoNodeAvailableException;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use Symfony\Component\DomCrawler\Crawler;
use Psr\Log\LoggerInterface;
use App\Entity\CrawlStat;
use Doctrine\ORM\EntityManagerInterface;

use Symfony\Component\Console\Output\OutputInterface;

/**
 * Service class for crawling ODIS (Ocean Data and Information System) data sources.
 * 
 * This crawler fetches records from the ODIS catalogue, follows sitemaps, 
 * extracts JSON-LD metadata from HTML pages or JSON files, and indexes 
 * the processed data into Elasticsearch.
 */
class OdisCrawler
{
    private GuzzleClient $httpClient;
    private ClientInterface $esClient;
    private string $esIndex;
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

    /**
     * OdisCrawler constructor.
     *
     * @param ClientInterface        $esClient      Elasticsearch client
     * @param LoggerInterface        $logger        Logger service
     * @param EntityManagerInterface $entityManager Doctrine entity manager
     * @param RobotsTxtManager       $robotsManager Service to handle robots.txt rules
     * @param string                 $esIndex       Target Elasticsearch index name
     * @param GuzzleClient|null      $httpClient    Optional Guzzle HTTP client
     */
    public function __construct(
        ClientInterface $esClient,
        LoggerInterface $logger,
        EntityManagerInterface $entityManager,
        RobotsTxtManager $robotsManager,
        string $esIndex,
        ?GuzzleClient $httpClient = null
    ) {
        $this->esClient = $esClient;
        $this->logger = $logger;
        $this->entityManager = $entityManager;
        $this->robotsManager = $robotsManager;
        $this->esIndex = $esIndex;
        $this->httpClient = $httpClient ?: new GuzzleClient([
            'timeout'  => 15.0,
            'verify' => false,
            'headers' => [
                'Accept' => 'text/html,application/json,application/ld+json;q=0.9,*/*;q=0.8',
                'User-Agent' => 'ODIS https://search.odis.org',
            ]
        ]);
    }

    /**
     * Sets the console output for real-time progress reporting.
     *
     * @param OutputInterface|null $output
     */
    public function setOutput(?OutputInterface $output): void
    {
        $this->output = $output;
    }

    /**
     * Sets the command line string used to start the crawl (for logging purposes).
     *
     * @param string $commandLine
     */
    public function setCommandLine(string $commandLine): void
    {
        $this->commandLine = $commandLine;
    }

    /**
     * Sets a limit on the number of data sources to process.
     *
     * @param int $limit
     */
    public function setLimit(int $limit): void
    {
        $this->limit = $limit;
    }

    /**
     * Internal logging method that sends messages to the logger and console output.
     *
     * @param string $message The message to log
     * @param string $level   Log level (info, warning, error, debug)
     */
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

    /**
     * Executes the crawling process.
     *
     * @param array|null $specificIds Optional list of data source IDs to crawl specifically
     * @param array|null $skipIds     Optional list of data source IDs to skip
     * 
     * @throws \RuntimeException If index existence check fails
     */
    public function run(
        ?array $specificIds = null,
        ?array $skipIds = null
    ): void {
        gc_collect_cycles(); // Cleanup memory before start
        $this->visitedSitemaps = [];
        $this->nodesFoundCount = 0;
        $this->pagesCrawledCount = 0;
        $this->validJsonLdsCount = 0;
        $this->invalidJsonLdsCount = 0;
        $this->crawlerErrorsCount = 0;
        $this->errorDetails = [];
        $this->lastUpdateTimestamp = time();

        // Initialize crawl statistics entity
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
            // Ensure Elasticsearch index is ready
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

        // Retrieve data source records from ODIS API
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

        // Filter IDs based on skip list
        if ($skipIds !== null && !empty($skipIds)) {
            $this->log("Skipping IDs: " . implode(', ', $skipIds));
            $dsIds = array_filter($dsIds, fn($id) => !in_array($id, $skipIds));
            // Update nodes found count if we skipped some
            if ($specificIds === null) {
                $this->nodesFoundCount = count($dsIds);
            }
        }

        // Apply limit if specified
        if ($this->limit > 0 && empty($specificIds)) {
            $this->log("Applying limit of {$this->limit} datasources");
            $dsIds = array_slice($dsIds, 0, $this->limit);
            $this->nodesFoundCount = count($dsIds);
        }

        // Process each data source
        foreach ($dsIds as $id) {
            $record = $records[$id] ?? null;
            $this->processDatasource($id, $record);
            gc_collect_cycles(); // Cleanup memory after each datasource
        }

        $this->saveStats();
    }

    /**
     * Finalizes and saves crawl statistics to the database.
     *
     * @param string $status Final status of the crawl (completed, failed, etc.)
     */
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

    /**
     * Periodically updates the progress of the crawl in the database.
     * 
     * Updates occur at most once every 2 seconds to avoid database overhead.
     */
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

    /**
     * Clears the Elasticsearch index.
     * 
     * @throws \RuntimeException If index deletion fails or Elasticsearch is unreachable
     */
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
            $message = "Elasticsearch connection failed: " . $e->getMessage();
            $message .= 'Solution: Please check your ELASTICSEARCH_URL in .env.local and ensure the Elasticsearch service is running.';
            $this->log($message, 'error');
            throw new \RuntimeException($message, 0, $e);
        } catch (\Exception $e) {
            $message = "Failed to clear Elasticsearch index: " . $e->getMessage();
            $this->log($message, 'error');
            throw new \RuntimeException($message, 0, $e);
        }
    }

    /**
     * Returns the Elasticsearch index mapping configuration.
     *
     * @return array
     */
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
                'image' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'schema:image' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'logo' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'schema:logo' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'thumbnail' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'schema:thumbnail' => ['type' => 'text', 'fields' => ['keyword' => ['type' => 'keyword']]],
                'contentUrl' => ['type' => 'keyword'],
                'schema:contentUrl' => ['type' => 'keyword'],
                'data' => [
                    'type' => 'object',
                    'dynamic' => true
                ],
                '@context' => ['type' => 'flattened']
            ]
        ];
    }

    /**
     * Creates the Elasticsearch index with the defined mapping.
     */
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

    /**
     * Ensures that the Elasticsearch index exists, creating it if necessary.
     */
    private function ensureIndexExists(): void
    {
        $params = ['index' => $this->esIndex];
        if (!$this->esClient->indices()->exists($params)->asBool()) {
            $this->createIndex();
        }
    }

    /**
     * Fetches data source records from the ODIS catalogue API.
     *
     * @return array Associative array of records indexed by their ID
     */
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

    /**
     * Processes a single data source.
     * 
     * It either uses a pre-fetched record or fetches the data source details 
     * from the ODIS view page. Then it proceeds to process sitemaps or 
     * JSON/SiteGraph URLs found.
     *
     * @param string     $id     Data source ID
     * @param array|null $record Optional pre-fetched record data
     */
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
            // Deduplicate errors
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

        // If we don't have the architecture URL, we try to scrape it from the ODIS view page
        if (!$archUrl) {
            $url = $this->viewBaseUrl . $id;
            $this->log("Processing ID $id from $url (no pre-fetched record)", 'debug');

            try {
                $response = $this->httpClient->get($url);
                $html = (string) $response->getBody();
                $crawler = new Crawler($html);

                // Look for ODIS-Arch URL and Type in the table
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

        // If an architecture URL was found, proceed with crawling based on type
        if ($archUrl) {
            // Respect robots.txt
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
                // Fallback for JSON files if type is not explicit
                if (str_contains($archUrl, '.json')) {
                    $this->fetchAndIndexJson($archUrl);
                }
            }
        } else {
            $this->log("No ODIS-Arch URL found for ID $id", 'debug');
        }
    }

    /**
     * Processes a sitemap XML file, following nested sitemaps or crawling URLs.
     *
     * @param string $sitemapUrl URL of the sitemap
     */
    private function processSitemap(string $sitemapUrl): void
    {
        // Prevent infinite loops in case of circular sitemap references
        if (in_array($sitemapUrl, $this->visitedSitemaps)) {
            $this->log("Sitemap already visited: $sitemapUrl. Skipping to prevent infinite loop.", 'warning');
            return;
        }
        $this->visitedSitemaps[] = $sitemapUrl;

        // Respect robots.txt
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
                // Special handling for common GeoNetwork sitemap location misconfigurations
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

            // If the sitemap doesn't look like XML, it might be a direct content page
            if (!str_contains($contentType, 'xml') && !str_starts_with(trim($xml), '<?xml') && !str_starts_with(trim($xml), '<sitemapindex') && !str_starts_with(trim($xml), '<urlset')) {
                $this->log("Sitemap URL $sitemapUrl returned non-XML content ($contentType). Treating as potential JSON-LD page.", 'warning');
                $this->fetchAndIndexJson($sitemapUrl);
                return;
            }

            try {
                $sitemap = new \SimpleXMLElement($xml);
            } catch (\Exception $e) {
                // If XML parsing fails, maybe it's a content page despite our checks
                $this->log("Failed to parse sitemap $sitemapUrl as XML. Attempting to treat as content page.", 'debug');
                $this->fetchAndIndexJson($sitemapUrl);
                return;
            }
            
            // Register namespaces for XPath queries
            $namespaces = $sitemap->getNamespaces(true);
            $nsPrefix = '';
            if (isset($namespaces[''])) {
                $sitemap->registerXPathNamespace('s', $namespaces['']);
                $nsPrefix = 's:';
            }

            // Handle Sitemap Index (list of other sitemaps)
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

            // Handle Standard Sitemap (list of URLs)
            $locs = $sitemap->xpath("//{$nsPrefix}loc");
            
            // Release XML string memory
            unset($xml);
            
            foreach ($locs as $loc) {
                // Respect per-datasource limit
                if ($this->limit > 0 && $this->processedInCurrentDatasource >= $this->limit) {
                    $this->log("Limit of {$this->limit} reached for datasource {$this->currentDatasourceId}. Skipping remaining sitemap URLs.", 'info');
                    break;
                }
                $url = trim((string) $loc);
                if (empty($url)) continue;
                $this->fetchAndIndexJson($url);
                
                // Explicitly cleanup memory in sitemap loop to prevent OOM
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

    /**
     * Normalizes data for safe indexing in Elasticsearch.
     * 
     * Wraps scalar values in objects to avoid mapping conflicts under the 'data' field,
     * which is configured as an object type in the index mapping.
     *
     * @param array $data The data to normalize
     * @return array Normalized data
     */
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

    /**
     * Normalizes fields that can be either strings or arrays/objects into a consistent array of objects.
     * 
     * This is useful for fields like 'creator', 'author', etc., which are often inconsistently structured.
     *
     * @param mixed $value The field value to normalize
     * @return array|null Normalized array of objects, or null
     */
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

    /**
     * Provides a human-readable solution for common crawler/Elasticsearch errors.
     *
     * @param string $message The error message
     * @return string Suggested solution
     */
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

    /**
     * Fetches a URL and indexes the JSON-LD data found within.
     *
     * Handles both direct JSON responses and HTML pages containing JSON-LD scripts.
     * Supports SiteGraphs and top-level JSON arrays by expanding them.
     *
     * @param string $url The URL to fetch and index
     * @throws GuzzleException
     */
    public function fetchAndIndexJson(string $url): void
    {
        // Respect robots.txt
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
            // Use streaming to avoid loading massive bodies into memory all at once
            $response = $this->httpClient->get($url, ['stream' => true]);
            $bodyStream = $response->getBody();
            
            // Check content length if provided by the server
            $contentLength = (int) $response->getHeaderLine('Content-Length');
            if ($contentLength > 50 * 1024 * 1024) {
                $this->log("Large response detected ($contentLength bytes). Processing with caution.", 'warning');
            }

            $contentType = $response->getHeaderLine('Content-Type');
            
            // Get content and try to free the stream ASAP
            $body = $bodyStream->getContents();
            
            $data = null;
            $this->log("Processing body with content type: $contentType", 'debug');
            
            if (
                str_contains($contentType, 'application/json')
                || str_contains($contentType, 'application/ld+json')
            ) {
                // Case 1: JSON or JSON-LD response
                $body = trim($body);
                // Remove UTF-8 BOM (Byte Order Mark) if present
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
                // Case 2: HTML response
                $data = $this->extractJsonLdFromHtml($body);
            } else {
                // Case 3: Unknown content type, attempt both
                $data = json_decode($body, true);
                if (!$data) {
                    $data = $this->extractJsonLdFromHtml($body);
                }
                // Free memory immediately
                unset($body);
            }
            
            // Process the extracted data (expand graphs/lists if necessary)
            if ($data) {
                // Determine if this is a SiteGraph or a collection of items
                $isTopLevelList = is_array($data) && array_is_list($data);
                $isGraph = (
                        isset($data['@graph'])
                        && is_array($data['@graph'])
                    )
                    || (
                        isset($data['itemListElement'])
                        && is_array($data['itemListElement'])
                    )
                    || (
                        isset($data['dataset'])
                        && is_array($data['dataset'])
                    )
                    || $isTopLevelList;

                // Special case for ODIS site graphs that might use different wrapping keys
                if (
                    !$isGraph
                    && isset($data['graph'])
                    && is_array($data['graph'])
                ) {
                    $isGraph = true;
                    $graph = $data['graph'];
                } elseif (
                    !$isGraph
                    && str_ends_with($url, '.json')
                    && count($data) > 0
                    && !isset($data['@type'])
                ) {
                    // If it's a large associative array with many keys and no @type, it's likely a collection.
                    $isGraph = true;
                    $graph = $data;
                } elseif ($isGraph) {
                    if ($isTopLevelList) {
                        $graph = $data;
                    } elseif (
                        isset($data['@graph'])
                        && is_array($data['@graph'])
                    ) {
                        $graph = $data['@graph'];
                    } elseif (
                        isset($data['itemListElement'])
                        && is_array($data['itemListElement'])
                    ) {
                        $graph = $data['itemListElement'];
                    } elseif (
                        isset($data['dataset'])
                        && is_array($data['dataset'])
                    ) {
                        $graph = $data['dataset'];
                    }
                }

                if ($isGraph) {
                    $this->log("Graph detected at $url (" . count($graph) . " items). Indexing individually.", 'info');
                    unset($data); // Free parent immediately
                    foreach ($graph as $index => $item) {
                        if (
                            $this->limit > 0
                            && $this->validJsonLdsCount >= $this->limit
                        ) {
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
                            'inLanguage', 'schema:inLanguage', 'datePublished', 'schema:datePublished',
                            'image', 'schema:image', 'logo', 'schema:logo', 
                            'thumbnail', 'schema:thumbnail', 'contentUrl', 'schema:contentUrl',
                            'caption', 'schema:caption', 'headline', 'schema:headline'
                        ];

                        $type = $item['@type']['value'] ?? $item['@type'] ?? '';

                        // Skip BreadcrumbList as it's not a valid ODIS type for indexing
                        if (
                            $type === 'BreadcrumbList'
                            || $type === 'schema:BreadcrumbList'
                        ) {
                            $this->log("Skipping BreadcrumbList at $url", 'debug');
                            continue;
                        }

                        // Handle ListItem: extract the actual 'item' content if present
                        if (
                            $type === 'ListItem'
                            || $type === 'schema:ListItem'
                        ) {
                            if (isset($item['item']) && is_array($item['item'])) {
                                $item = $item['item'];
                                $type = $item['@type']['value'] ?? $item['@type'] ?? '';
                            }
                        }

                        // Re-check type after possible ListItem unwrap
                        if (
                            $type === 'BreadcrumbList'
                            || $type === 'schema:BreadcrumbList'
                        ) {
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

                    // If item itself has a 'url' (like ImageObject), use it as the root 'url' if it's a string
                    if (isset($item['url'])) {
                        $itemUrl = is_array($item['url']) ? ($item['url']['value'] ?? null) : $item['url'];
                        if (is_string($itemUrl)) {
                            $body['url'] = $itemUrl;
                        }
                    }

                    foreach ($rootFields as $field) {
                            if (isset($item[$field])) {
                                if (
                                    is_array($item[$field])
                                    && isset($item[$field]['value'])
                                ) {
                                    $val = $item[$field]['value'];
                                } else {
                                    $val = $item[$field];
                                }

                                // If the field is an object (like an ImageObject), try to extract its URL
                                if (is_array($val)) {
                                    if (isset($val['url'])) {
                                        $val = is_array($val['url']) ? ($val['url']['value'] ?? $val['url']) : $val['url'];
                                    } elseif (isset($val['contentUrl'])) {
                                        $val = is_array($val['contentUrl']) ? ($val['contentUrl']['value'] ?? $val['contentUrl']) : $val['contentUrl'];
                                    }
                                    
                                    // If it's still an array after trying to extract URL, stringify it
                                    if (is_array($val)) {
                                        if (array_is_list($val)) {
                                            if (
                                                $field === '@type'
                                                || $field === 'schema:@type'
                                            ) {
                                                // Keep as array for @type to allow multiple types in Elasticsearch
                                                $val = array_map(function($v) {
                                                    if (is_array($v) && isset($v['name'])) {
                                                        return is_array($v['name']) ? ($v['name']['value'] ?? json_encode($v['name'])) : $v['name'];
                                                    }
                                                    return is_array($v) ? ($v['value'] ?? json_encode($v)) : $v;
                                                }, $val);
                                            } else {
                                                $val = implode(', ', array_map(function($v) {
                                                    if (is_array($v) && isset($v['name'])) {
                                                        return is_array($v['name']) ? ($v['name']['value'] ?? json_encode($v['name'])) : $v['name'];
                                                    }
                                                    return is_array($v) ? ($v['value'] ?? json_encode($v)) : $v;
                                                }, $val));
                                            }
                                        } else {
                                            $val = $val['name'] ?? $val['value'] ?? json_encode($val);
                                            if (is_array($val)) {
                                                $val = $val['value'] ?? json_encode($val);
                                            }
                                        }
                                    }
                                }

                                if (
                                    (
                                        $field === 'keywords'
                                        || $field === 'schema:keywords'
                                    )
                                    && is_array($val)
                                ) {
                                    $val = implode(', ', array_map(function($k) {
                                        return is_array($k) ? ($k['value'] ?? json_encode($k)) : $k;
                                    }, $val));
                                }
                            
                                // If title was found, map it to name for consistent indexing
                                if (
                                    $field === 'title'
                                    || $field === 'schema:title'
                                ) {
                                    $body['name'] = $val;
                                }
                            
                                $body[$field] = $val;
                            }
                        }

                        $params = [
                            'index' => $this->esIndex,
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
                        'index' => $this->esIndex,
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
                    if (
                        $type === 'BreadcrumbList'
                        || $type === 'schema:BreadcrumbList'
                    ) {
                        $this->log("Skipping BreadcrumbList at $url", 'debug');
                        return;
                    }

                    // Special case for ItemList and ListItem: unwrap if they contain an 'item'
                    if (
                        (
                            $type === 'ListItem'
                            || $type === 'schema:ListItem'
                        )
                        && isset($normalizedData['item'])
                    ) {
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
                    if (
                        $type === 'BreadcrumbList'
                        || $type === 'schema:BreadcrumbList'
                    ) {
                        $this->log("Skipping BreadcrumbList (unwrapped) at $url", 'debug');
                        return;
                    }

                    $params['body']['data'] = $normalizedData;

                    // If item itself has a 'url', use it as the root 'url' if it's a string
                    if (isset($normalizedData['url'])) {
                        $itemUrl = is_array($normalizedData['url']) ? ($normalizedData['url']['value'] ?? null) : $normalizedData['url'];
                        if (is_string($itemUrl)) {
                            $params['body']['url'] = $itemUrl;
                        }
                    }

                    // Extract root-level fields from wrapped objects before indexing
                    $rootFields = [
                        'name', 'schema:name', 'title', 'schema:title',
                        'description', 'schema:description', 
                        '@type', 'schema:@type', 'keywords', 'schema:keywords',
                        'inLanguage', 'schema:inLanguage', 'datePublished', 'schema:datePublished',
                        'image', 'schema:image', 'logo', 'schema:logo', 
                        'thumbnail', 'schema:thumbnail', 'contentUrl', 'schema:contentUrl',
                        'caption', 'schema:caption', 'headline', 'schema:headline'
                    ];
                    foreach ($rootFields as $field) {
                        if (isset($normalizedData[$field])) {
                            if (
                                is_array($normalizedData[$field])
                                && isset($normalizedData[$field]['value'])
                            ) {
                                $val = $normalizedData[$field]['value'];
                            } else {
                                $val = $normalizedData[$field];
                            }

                            // If the field is an object (like an ImageObject), try to extract its URL
                            if (is_array($val)) {
                                if (isset($val['url'])) {
                                    $val = is_array($val['url']) ? ($val['url']['value'] ?? $val['url']) : $val['url'];
                                } elseif (isset($val['contentUrl'])) {
                                    $val = is_array($val['contentUrl']) ? ($val['contentUrl']['value'] ?? $val['contentUrl']) : $val['contentUrl'];
                                }

                                // If it's still an array after trying to extract URL, stringify it
                                if (is_array($val)) {
                                    if (array_is_list($val)) {
                                        if (
                                            $field === '@type'
                                            || $field === 'schema:@type'
                                        ) {
                                            // Keep as array for @type to allow multiple types in Elasticsearch
                                            $val = array_map(function($v) {
                                                if (is_array($v) && isset($v['name'])) {
                                                    return is_array($v['name']) ? ($v['name']['value'] ?? json_encode($v['name'])) : $v['name'];
                                                }
                                                return is_array($v) ? ($v['value'] ?? json_encode($v)) : $v;
                                            }, $val);
                                        } else {
                                            $val = implode(', ', array_map(function($v) {
                                                if (is_array($v) && isset($v['name'])) {
                                                    return is_array($v['name']) ? ($v['name']['value'] ?? json_encode($v['name'])) : $v['name'];
                                                }
                                                return is_array($v) ? ($v['value'] ?? json_encode($v)) : $v;
                                            }, $val));
                                        }
                                    } else {
                                        $val = $val['name'] ?? $val['value'] ?? json_encode($val);
                                        if (is_array($val)) {
                                            $val = $val['value'] ?? json_encode($val);
                                        }
                                    }
                                }
                            }

                            if (
                                (
                                    $field === 'keywords'
                                    || $field === 'schema:keywords'
                                )
                                && is_array($val)
                            ) {
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
            if (
                str_contains($message, '400 Bad Request')
                || str_contains($message, 'document_parsing_exception')
            ) {
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

    /**
     * Extracts JSON-LD metadata from an HTML string.
     * 
     * Supports multiple script tags, handles common syntax errors in source data, 
     * and expands graph/list structures.
     *
     * @param string $html The HTML content
     * @return array|null Array of extracted JSON-LD objects, or null
     */
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
                    if (empty($json)) {
                        return;
                    }
                    
                    $decoded = json_decode($json, true);
                    if ($decoded === null
                        && !empty($json)
                    ) {
                        // Fallback: try decoding HTML entities and removing control characters
                        $cleaned = html_entity_decode(
                            preg_replace(
                                '/[\x00-\x1F\x7F]/',
                                ' ',
                                $json
                            )
                        );
                        $decoded = json_decode($cleaned, true);
                        
                        if ($decoded === null) {
                            // Aggressive fallback: handle unescaped internal double quotes in some common GeoNetwork patterns
                            // e.g. "alternateTitle": "Gas pipelines ... "as built" in ..."
                            // We look for " followed by characters then " then characters then " where it's part of a value
                            // This is risky but helps with broken source data
                            $cleanedAggressive = preg_replace_callback(
                                '/(": ")(.*?)("(?:\s*)[,}\]])/s',
                                function($matches) {
                                    $value = $matches[2];
                                    // Escape internal quotes in the value
                                    $value = str_replace('"', '\"', $value);
                                    return $matches[1] . $value . $matches[3];
                                },
                                $cleaned
                            );
                            $decoded = json_decode($cleanedAggressive, true);
                        }
                    }

                    if ($decoded === null && !empty($json)) {
                        $jsonError = json_last_error_msg();
                        $solution = $this->getSolutionForError($jsonError);
                        $this->log("Failed to decode JSON-LD snippet: $jsonError. $solution (first 100 chars: " . substr($json, 0, 100) . ")", 'warning');
                    }
                    // Clear large string from memory as soon as possible
                    unset($json);
                    
                    if ($decoded) {
                        if (isset($decoded['@graph'])
                            && is_array($decoded['@graph'])
                        ) {
                            foreach ($decoded['@graph'] as $item) {
                                $results[] = $item;
                            }
                        } elseif (
                            isset($decoded['@type'])
                            && (is_string($decoded['@type']) ? $decoded['@type'] === 'ItemList' : in_array('ItemList', $decoded['@type']))
                            && isset($decoded['itemListElement'])
                        ) {
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
                        } elseif (
                            is_array($decoded)
                            && array_is_list($decoded)
                        ) {
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
