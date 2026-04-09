<?php

namespace App\Tests\Service;

use App\Service\OdisCrawler;
use App\Service\RobotsTxtManager;
use Doctrine\ORM\EntityManagerInterface;
use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientInterface;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Output\NullOutput;

class OdisCrawlerTest extends TestCase
{
    private $esClient;
    private $logger;
    private $entityManager;
    private $robotsManager;
    private $crawler;

    protected function setUp(): void
    {
        $this->esClient = $this->createMock(ClientInterface::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->entityManager = $this->createMock(EntityManagerInterface::class);
        $this->robotsManager = $this->createMock(RobotsTxtManager::class);

        $this->crawler = new OdisCrawler(
            $this->esClient,
            $this->logger,
            $this->entityManager,
            $this->robotsManager
        );
        $this->crawler->setOutput(new NullOutput());
    }

    public function testNormalizeDataForSafeIndexingWrapsScalars()
    {
        $data = [
            'name' => 'Test Name',
            'description' => 'Test Description',
            'nested' => [
                'field' => 'Nested Value'
            ]
        ];

        $result = $this->crawler->normalizeDataForSafeIndexing($data);

        $this->assertIsArray($result['name']);
        $this->assertEquals('Test Name', $result['name']['value']);
        $this->assertIsArray($result['description']);
        $this->assertEquals('Test Description', $result['description']['value']);
        $this->assertIsArray($result['nested']['field']);
        $this->assertEquals('Nested Value', $result['nested']['field']['value']);
    }

    public function testGetIndexMappingContainsTemporalCoverage()
    {
        $mapping = $this->crawler->getIndexMapping();
        $properties = $mapping['properties'];

        $this->assertArrayHasKey('temporalCoverage', $properties);
        $this->assertEquals('flattened', $properties['temporalCoverage']['type']);
        $this->assertArrayHasKey('schema:temporalCoverage', $properties);
        $this->assertEquals('flattened', $properties['schema:temporalCoverage']['type']);
        $this->assertArrayHasKey('keywords', $properties);
        $this->assertEquals('text', $properties['keywords']['type']);
    }

    public function testUnwrapListItem()
    {
        $data = [
            '@type' => 'ListItem',
            'item' => [
                '@type' => 'Dataset',
                'name' => 'Inner Dataset',
                'description' => 'Inner Description'
            ],
            'position' => 1
        ];

        $normalizedData = $this->crawler->normalizeDataForSafeIndexing($data);
        
        // This is the logic we added to the crawler
        $type = $normalizedData['@type']['value'] ?? $normalizedData['@type'] ?? '';
        if (($type === 'ListItem' || $type === 'schema:ListItem') && isset($normalizedData['item'])) {
            $innerItem = $normalizedData['item'];
            foreach ($normalizedData as $k => $v) {
                if ($k !== 'item' && !isset($innerItem[$k])) {
                    $innerItem[$k] = $v;
                }
            }
            $normalizedData = $innerItem;
        }

        $this->assertEquals('Dataset', $normalizedData['@type']['value']);
        $this->assertEquals('Inner Dataset', $normalizedData['name']['value']);
        $this->assertEquals(1, $normalizedData['position']['value']);
        $this->assertArrayNotHasKey('item', $normalizedData);
    }

    public function testMappingDisablesDataFields()
    {
        $mapping = $this->crawler->getIndexMapping();
        $dynamicTemplates = $mapping['dynamic_templates'];

        $found = false;
        foreach ($dynamicTemplates as $template) {
            if (isset($template['data_fields'])) {
                $this->assertEquals('data.*', $template['data_fields']['path_match']);
                $this->assertEquals('object', $template['data_fields']['mapping']['type']);
                $this->assertFalse($template['data_fields']['mapping']['enabled']);
                $found = true;
                break;
            }
        }
        $this->assertTrue($found, 'Template data_fields not found in mapping');
    }

    public function testFetchAndIndexGraph()
    {
        $url = 'https://example.org/graph.json';
        $graphData = [
            '@context' => 'https://schema.org',
            '@graph' => [
                [
                    '@id' => 'item1',
                    '@type' => 'Dataset',
                    'name' => 'Dataset One',
                    'description' => 'Description One'
                ],
                [
                    '@id' => 'item2',
                    '@type' => 'Dataset',
                    'name' => 'Dataset Two',
                    'description' => 'Description Two'
                ]
            ]
        ];

        // Mock HTTP client to return the graph
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/ld+json'], json_encode($graphData))
        ]);
        $handlerStack = HandlerStack::create($mock);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        // Mock ES client using a simple anonymous class that implements ClientInterface
        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        // Re-initialize crawler with mock HTTP client
        $this->crawler = new OdisCrawler(
            $esClientMock,
            $this->logger,
            $this->entityManager,
            $this->robotsManager,
            $httpClient
        );
        $this->crawler->setOutput(new NullOutput());

        // Mock robots manager to allow the URL
        $this->robotsManager->method('isAllowed')->willReturn(true);

        $this->crawler->fetchAndIndexJson($url);

        $this->assertCount(2, $esClientMock->indexCalls);
        $this->assertEquals('be95ec789e30b560e2b3ca0a35de2ba1', $esClientMock->indexCalls[0]['id'], 'First item ID mismatch');
        $this->assertEquals('Dataset One', $esClientMock->indexCalls[0]['body']['name']);
        $this->assertEquals('4a541827f64af78abed9a2c537622f32', $esClientMock->indexCalls[1]['id'], 'Second item ID mismatch');
        $this->assertEquals('Dataset Two', $esClientMock->indexCalls[1]['body']['name']);
        
        // Let's also verify that it works when IDs are missing (should use md5 of URL+index)
        $urlNoIds = 'https://example.org/no-ids.json';
        $graphNoIds = [
            '@graph' => [
                ['name' => 'No ID Item']
            ]
        ];
        $mock->append(new Response(200, ['Content-Type' => 'application/json'], json_encode($graphNoIds)));
        
        $esClientMock->indexCalls = [];
        $this->crawler->fetchAndIndexJson($urlNoIds);
        $this->assertCount(1, $esClientMock->indexCalls);
        $this->assertEquals(md5(md5($urlNoIds . '0')), $esClientMock->indexCalls[0]['id']);
        $this->assertEquals('No ID Item', $esClientMock->indexCalls[0]['body']['name']);
    }

    public function testFetchAndIndexListWrappedHtml()
    {
        $url = 'https://example.org/organisation/list-wrapped';
        $html = '<html><head>
<script type="application/ld+json">
[{"@context":"https://schema.org/","@type":"Organization","name":"Test Org","description":"Test Desc"}]
</script>
</head></html>';

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'text/html'], $html)
        ]);
        $handlerStack = HandlerStack::create($mock);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        $robotsManagerMock = $this->createMock(RobotsTxtManager::class);
        $robotsManagerMock->method('isAllowed')->willReturn(true);

        $crawler = new OdisCrawler(
            $esClientMock,
            $this->createMock(LoggerInterface::class),
            $this->createMock(EntityManagerInterface::class),
            $robotsManagerMock,
            $httpClient
        );
        $crawler->setOutput(new NullOutput());

        $crawler->fetchAndIndexJson($url);

        $this->assertCount(1, $esClientMock->indexCalls);
        $this->assertEquals('Test Org', $esClientMock->indexCalls[0]['body']['name']);
        $this->assertEquals('Test Desc', $esClientMock->indexCalls[0]['body']['description']);
        $this->assertEquals('Organization', $esClientMock->indexCalls[0]['body']['@type']);
    }

    public function testBreadcrumbListIsSkipped()
    {
        $url = 'https://example.org/page-with-breadcrumb';
        $graphData = [
            '@graph' => [
                [
                    '@type' => 'BreadcrumbList',
                    'itemListElement' => [
                        ['@type' => 'ListItem', 'position' => 1, 'name' => 'Home']
                    ]
                ],
                [
                    '@type' => 'Dataset',
                    'name' => 'Real Dataset'
                ]
            ]
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/ld+json'], json_encode($graphData))
        ]);
        $handlerStack = HandlerStack::create($mock);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        $robotsManagerMock = $this->createMock(RobotsTxtManager::class);
        $robotsManagerMock->method('isAllowed')->willReturn(true);

        $crawler = new OdisCrawler(
            $esClientMock,
            $this->createMock(LoggerInterface::class),
            $this->createMock(EntityManagerInterface::class),
            $robotsManagerMock,
            $httpClient
        );
        $crawler->setOutput(new NullOutput());

        $crawler->fetchAndIndexJson($url);

        // Only the Dataset should be indexed, not the BreadcrumbList
        $this->assertCount(1, $esClientMock->indexCalls);
        $this->assertEquals('Real Dataset', $esClientMock->indexCalls[0]['body']['name']);
        $this->assertEquals('Dataset', $esClientMock->indexCalls[0]['body']['@type']);
    }

    public function testBreadcrumbListInsideListItemIsSkipped()
    {
        $url = 'https://example.org/page-with-nested-breadcrumb';
        $graphData = [
            '@graph' => [
                [
                    '@type' => 'ListItem',
                    'item' => [
                        '@type' => 'BreadcrumbList',
                        'name' => 'Nested Breadcrumb'
                    ]
                ],
                [
                    '@type' => 'ListItem',
                    'item' => [
                        '@type' => 'Dataset',
                        'name' => 'Nested Dataset'
                    ]
                ]
            ]
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/ld+json'], json_encode($graphData))
        ]);
        $handlerStack = HandlerStack::create($mock);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        $robotsManagerMock = $this->createMock(RobotsTxtManager::class);
        $robotsManagerMock->method('isAllowed')->willReturn(true);

        $crawler = new OdisCrawler(
            $esClientMock,
            $this->createMock(LoggerInterface::class),
            $this->createMock(EntityManagerInterface::class),
            $robotsManagerMock,
            $httpClient
        );
        $crawler->setOutput(new NullOutput());

        $crawler->fetchAndIndexJson($url);

        // Only the Dataset should be indexed
        $this->assertCount(1, $esClientMock->indexCalls);
        $this->assertEquals('Nested Dataset', $esClientMock->indexCalls[0]['body']['name']);
    }

    public function testFetchAndIndexDatasetKeyCollection()
    {
        $url = 'https://example.org/datasets.json';
        $datasetData = [
            'dataset' => [
                [
                    'title' => 'Dataset One',
                    'description' => 'Description One',
                    'identifier' => 'id1',
                    '@type' => 'dcat:Dataset'
                ],
                [
                    'title' => 'Dataset Two',
                    'description' => 'Description Two',
                    'identifier' => 'id2',
                    '@type' => 'dcat:Dataset'
                ]
            ]
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode($datasetData))
        ]);
        $handlerStack = HandlerStack::create($mock);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        $robotsManagerMock = $this->createMock(RobotsTxtManager::class);
        $robotsManagerMock->method('isAllowed')->willReturn(true);

        $crawler = new OdisCrawler(
            $esClientMock,
            $this->createMock(LoggerInterface::class),
            $this->createMock(EntityManagerInterface::class),
            $robotsManagerMock,
            $httpClient
        );
        $crawler->setOutput(new NullOutput());

        $crawler->fetchAndIndexJson($url);

        // Both datasets from the 'dataset' array should be indexed
        $this->assertCount(2, $esClientMock->indexCalls);
        $this->assertEquals('Dataset One', $esClientMock->indexCalls[0]['body']['name']);
        $this->assertEquals('Dataset Two', $esClientMock->indexCalls[1]['body']['name']);
    }

    public function testFetchAndIndexImageObject()
    {
        $url = 'https://example.org/image.json';
        $imageData = [
            '@context' => 'https://schema.org',
            '@type' => 'ImageObject',
            'url' => 'https://example.org/image.jpg',
            'width' => 600,
            'height' => 400,
            'inLanguage' => 'en'
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/ld+json'], json_encode($imageData))
        ]);
        $handlerStack = HandlerStack::create($mock);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        $robotsManagerMock = $this->createMock(RobotsTxtManager::class);
        $robotsManagerMock->method('isAllowed')->willReturn(true);

        $crawler = new OdisCrawler(
            $esClientMock,
            $this->createMock(LoggerInterface::class),
            $this->createMock(EntityManagerInterface::class),
            $robotsManagerMock,
            $httpClient
        );
        $crawler->setOutput(new NullOutput());

        $crawler->fetchAndIndexJson($url);

        $this->assertCount(1, $esClientMock->indexCalls);
        $body = $esClientMock->indexCalls[0]['body'];
        $this->assertEquals('ImageObject', $body['@type']);
        $this->assertEquals('https://example.org/image.jpg', $body['url']);
        $this->assertEquals('en', $body['inLanguage']);
        
        // Test nested ImageObject extraction (e.g. logo)
        $urlNested = 'https://example.org/org.json';
        $orgData = [
            '@type' => 'Organization',
            'name' => 'Test Org',
            'logo' => [
                '@type' => 'ImageObject',
                'url' => 'https://example.org/logo.png'
            ]
        ];
        $mock->append(new Response(200, ['Content-Type' => 'application/json'], json_encode($orgData)));
        
        $esClientMock->indexCalls = [];
        $crawler->fetchAndIndexJson($urlNested);
        
        $this->assertCount(1, $esClientMock->indexCalls);
        $body = $esClientMock->indexCalls[0]['body'];
        $this->assertEquals('Test Org', $body['name']);
        $this->assertEquals('https://example.org/logo.png', $body['logo']);
    }

    public function testFetchAndIndexImageObjectWithName()
    {
        $url = 'https://example.org/image-with-name.json';
        $imageData = [
            '@context' => 'https://schema.org',
            '@type' => 'ImageObject',
            'name' => 'Beautiful Ocean View',
            'description' => 'A photo of the Atlantic Ocean during sunset.',
            'contentUrl' => 'https://example.org/ocean.jpg',
            'url' => 'https://example.org/image/1'
        ];

        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/ld+json'], json_encode($imageData))
        ]);
        $handlerStack = HandlerStack::create($mock);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        $robotsManagerMock = $this->createMock(RobotsTxtManager::class);
        $robotsManagerMock->method('isAllowed')->willReturn(true);

        $crawler = new OdisCrawler(
            $esClientMock,
            $this->createMock(LoggerInterface::class),
            $this->createMock(EntityManagerInterface::class),
            $robotsManagerMock,
            $httpClient
        );
        $crawler->setOutput(new NullOutput());

        $crawler->fetchAndIndexJson($url);

        $this->assertCount(1, $esClientMock->indexCalls);
        $body = $esClientMock->indexCalls[0]['body'];
        
        $this->assertEquals('ImageObject', $body['@type']);
        $this->assertEquals('Beautiful Ocean View', $body['name']);
        $this->assertEquals('A photo of the Atlantic Ocean during sunset.', $body['description']);
        $this->assertEquals('https://example.org/ocean.jpg', $body['contentUrl']);
        $this->assertEquals('https://example.org/image/1', $body['url']);

        // Test with headline and caption
        $urlExtra = 'https://example.org/image-extra.json';
        $extraData = [
            '@type' => 'ImageObject',
            'headline' => 'Ocean Sunset',
            'caption' => 'A wonderful sunset view'
        ];
        $mock->append(new Response(200, ['Content-Type' => 'application/json'], json_encode($extraData)));
        $esClientMock->indexCalls = [];
        $crawler->fetchAndIndexJson($urlExtra);
        
        $this->assertCount(1, $esClientMock->indexCalls);
        $bodyExtra = $esClientMock->indexCalls[0]['body'];
        $this->assertEquals('Ocean Sunset', $bodyExtra['headline']);
        $this->assertEquals('A wonderful sunset view', $bodyExtra['caption']);
    }

    public function testFetchAndIndexMultiTypeArray()
    {
        $mockHandler = new MockHandler([
            new Response(200, ['Content-Type' => 'application/ld+json'], json_encode([
                '@context' => 'https://schema.org',
                '@type' => ['Event', 'BoatTrip'],
                'name' => 'Multi-type Event',
                'description' => 'An event that is also a boat trip.'
            ]))
        ]);
        $handlerStack = HandlerStack::create($mockHandler);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        $robotsManagerMock = $this->createMock(RobotsTxtManager::class);
        $robotsManagerMock->method('isAllowed')->willReturn(true);

        $crawler = new OdisCrawler(
            $esClientMock,
            $this->createMock(LoggerInterface::class),
            $this->createMock(EntityManagerInterface::class),
            $robotsManagerMock,
            $httpClient
        );
        $crawler->setOutput(new NullOutput());

        $crawler->fetchAndIndexJson('https://example.com/multi-type');

        $this->assertCount(1, $esClientMock->indexCalls);
        $body = $esClientMock->indexCalls[0]['body'];
        
        // We expect an array for multi-type now
        $this->assertIsArray($body['@type']);
        $this->assertContains('Event', $body['@type']);
        $this->assertContains('BoatTrip', $body['@type']);
    }

    public function testFetchAndIndexDefinedTermKeywords()
    {
        $url = 'https://example.org/defined-terms.json';
        $data = [
            '@context' => 'https://schema.org',
            '@type' => 'Dataset',
            'name' => 'MEDIN Dataset',
            'keywords' => [
                [
                    '@type' => 'DefinedTerm',
                    'name' => 'Marine Biodiversity',
                    'termCode' => 'MD002'
                ],
                [
                    '@type' => 'DefinedTerm',
                    'name' => 'Renewable Energy Lease area'
                ]
            ],
            'schema:keywords' => [
                [
                    '@type' => 'DefinedTerm',
                    'name' => 'Post-Construction monitoring'
                ]
            ]
        ];

        $mockHandler = new MockHandler([
            new Response(200, ['Content-Type' => 'application/ld+json'], json_encode($data))
        ]);
        $handlerStack = HandlerStack::create($mockHandler);
        $httpClient = new GuzzleClient(['handler' => $handlerStack]);

        $esClientMock = new class implements ClientInterface {
            public $indexCalls = [];
            public function index(array $params) { $this->indexCalls[] = $params; return []; }
            public function getTransport(): \Elastic\Transport\Transport { throw new \Exception(); }
            public function getLogger(): \Psr\Log\LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return false; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return false; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(\Psr\Http\Message\RequestInterface $request) { throw new \Exception(); }
        };

        $robotsManagerMock = $this->createMock(RobotsTxtManager::class);
        $robotsManagerMock->method('isAllowed')->willReturn(true);

        $crawler = new OdisCrawler(
            $esClientMock,
            $this->createMock(LoggerInterface::class),
            $this->createMock(EntityManagerInterface::class),
            $robotsManagerMock,
            $httpClient
        );
        $crawler->setOutput(new NullOutput());

        $crawler->fetchAndIndexJson($url);

        $this->assertCount(1, $esClientMock->indexCalls);
        $body = $esClientMock->indexCalls[0]['body'];
        
        // Assert that keywords are flattened to their names
        $this->assertEquals('Marine Biodiversity, Renewable Energy Lease area', $body['keywords']);
        $this->assertEquals('Post-Construction monitoring', $body['schema:keywords']);
    }
}
