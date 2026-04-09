<?php

namespace App\Tests\Controller;

use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Elastic\Elasticsearch\ClientInterface;
use Elastic\Transport\Transport;
use Psr\Log\LoggerInterface;
use Http\Promise\Promise;
use Psr\Http\Message\RequestInterface;

class SearchControllerTest extends WebTestCase
{
    public function testSearchPageRenders()
    {
        $client = static::createClient();
        
        $mockEsClient = new class implements ClientInterface {
            public function getTransport(): Transport { throw new \Exception(); }
            public function getLogger(): LoggerInterface { throw new \Exception(); }
            public function setAsync(bool $async): ClientInterface { return $this; }
            public function getAsync(): bool { return false; }
            public function setElasticMetaHeader(bool $active): ClientInterface { return $this; }
            public function getElasticMetaHeader(): bool { return true; }
            public function setResponseException(bool $active): ClientInterface { return $this; }
            public function getResponseException(): bool { return true; }
            public function setServerless(bool $value): ClientInterface { return $this; }
            public function getServerless(): bool { return false; }
            public function sendRequest(RequestInterface $request) { throw new \Exception(); }
            
            public function ping() { return true; }
            public function search(array $params) {
                return [
                    'hits' => [
                        'total' => ['value' => 1],
                        'hits' => [
                            [
                                '_index' => 'odis_metadata',
                                '_id' => '1',
                                '_score' => 1.0,
                                '_source' => [
                                    'name' => 'Test Result',
                                    'description' => 'Test Description',
                                    'url' => 'https://example.com',
                                    '@type' => 'Dataset'
                                ]
                            ]
                        ]
                    ],
                    'aggregations' => [
                        'types' => [
                            'buckets' => [
                                ['key' => 'dataset', 'doc_count' => 1]
                            ]
                        ]
                    ]
                ];
            }
        };

        static::getContainer()->set(ClientInterface::class, $mockEsClient);

        $client->request('GET', '/search?q=test');

        $this->assertResponseIsSuccessful();
        $this->assertSelectorTextContains('h1', 'Search ODIS Metadata');
        $this->assertSelectorExists('.result-item');
        $this->assertAnySelectorTextContains('.result-item', 'Test Result');
    }
}
