<?php

namespace App\Controller;

use Elastic\Elasticsearch\Client;
use Elastic\Transport\Exception\NoNodeAvailableException;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

class SearchController extends AbstractController
{
    private Client $esClient;
    private string $esIndex = 'odis_metadata';

    public function __construct(Client $esClient)
    {
        $this->esClient = $esClient;
    }

    #[Route('/search', name: 'app_search')]
    public function search(Request $request): Response
    {
        try {
            // Check if Elasticsearch is alive
            $this->esClient->ping();
        } catch (NoNodeAvailableException $e) {
            return $this->render('search/error.html.twig', [
                'error' => 'Elasticsearch is currently unreachable.',
                'solution' => 'Please check the ELASTICSEARCH_URL in your configuration and ensure the Elasticsearch service is running.'
            ]);
        } catch (\Exception $e) {
            return $this->render('search/error.html.twig', [
                'error' => 'An error occurred while connecting to the search engine.',
                'solution' => $e->getMessage()
            ]);
        }

        $query = $request->query->get('q', '');
        $sort = $request->query->get('sort', '_score');
        $order = $request->query->get('order', 'desc');
        $typeFilter = $request->query->all('types');
        if (empty($typeFilter)) {
            $typeFilter = $request->query->get('type', '');
            if (!empty($typeFilter)) {
                $typeFilter = explode(',', $typeFilter);
            } else {
                $typeFilter = [];
            }
        }
        
        $page = max(1, (int) $request->query->get('page', 1));
        $limit = max(1, min(100, (int) $request->query->get('limit', 10)));
        
        // Safety: if limit is too high for complex queries, cap it aggressively
        // to stay within 128MB memory limit.
        if (!empty($query)) {
            $limit = min($limit, 15);
        } else {
            $limit = min($limit, 50);
        }

        gc_collect_cycles();
        
        $results = [];
        $facets = [];
        $totalResults = 0;

        $params = [
            'index' => $this->esIndex,
            'from'  => ($page - 1) * $limit,
            'size'  => $limit,
            'track_total_hits' => true,
            'body'  => [
                '_source' => [
                    'excludes' => [
                        'text', 'description', 'keywords', 'attendee.*', 'contributor.*', 'organizer.*',
                        'performer.*', 'person.*', 'provider.*', 'creator.*', 'author.*', 'hasCourseInstance.*',
                        'variableMeasured.*', 'distribution.*', 'subjectOf.*', 'about.*', 'funder.*', 'publisher.*',
                        'spatialCoverage.*', 'geo.*', 'potentialAction.*', 'identifier.*', 'image.*', 'mentions.*',
                        'startDate', 'endDate', 'location', 'arrivalBoatTerminal.*', 'departureBoatTerminal.*',
                        'schema:creator.*', 'schema:publisher.*', 'schema:provider.*', 'schema:funder.*', 'schema:author.*',
                        'schema:distribution.*', 'schema:subjectOf.*', 'schema:about.*', 'schema:spatialCoverage.*',
                        'schema:geo.*', 'schema:potentialAction.*', 'schema:identifier.*', 'schema:mentions.*',
                        'schema:startDate', 'schema:endDate', 'schema:location', 'schema:arrivalBoatTerminal.*', 'schema:departureBoatTerminal.*',
                        'subEvent.*', 'schema:subEvent.*', 'sdPublisher.*', 'schema:sdPublisher.*',
                        'datePublished', 'schema:datePublished', 'educationalCredentialAwarded', 'schema:educationalCredentialAwarded',
                        'inLanguage', 'schema:inLanguage'
                    ]
                ],
                'query' => [
                    'bool' => [
                        'must' => []
                    ]
                ],
                'aggs' => [
                    'types' => [
                        'terms' => ['field' => '@type.keyword', 'size' => 10]
                    ]
                ]
            ]
        ];

        if (!empty($query)) {
            $params['body']['query']['bool']['must'][] = [
                'query_string' => [
                    'query'  => '*' . $query . '*',
                    'fields' => ['name^3', 'schema:name^3', 'description', 'schema:description', 'keywords^2', 'schema:keywords^2', 'text', 'url', '@id', 'attendee.*', 'contributor.*', 'organizer.*', 'performer.*', 'person.*', 'provider.*', 'creator.*', 'author.*', 'schema:creator.*', 'schema:author.*', 'schema:publisher.*', 'schema:provider.*', 'schema:funder.*', 'startDate', 'endDate', 'location', 'arrivalBoatTerminal.*', 'departureBoatTerminal.*', 'subEvent.*', 'schema:subEvent.*', 'sdPublisher.*', 'schema:sdPublisher.*', 'educationalCredentialAwarded', 'schema:educationalCredentialAwarded', 'inLanguage', 'schema:inLanguage', '*.name', '*.description'],
                    'default_operator' => 'AND',
                    'allow_leading_wildcard' => true
                ]
            ];
            
            $params['body']['highlight'] = [
                'max_analyzed_offset' => 999999,
                'fields' => [
                    'name' => ['number_of_fragments' => 1, 'fragment_size' => 100],
                    'schema:name' => ['number_of_fragments' => 1, 'fragment_size' => 100],
                    'description' => ['number_of_fragments' => 1, 'fragment_size' => 150],
                    'schema:description' => ['number_of_fragments' => 1, 'fragment_size' => 150],
                    'keywords' => ['number_of_fragments' => 3, 'fragment_size' => 50],
                    'schema:keywords' => ['number_of_fragments' => 3, 'fragment_size' => 50],
                    'text' => ['number_of_fragments' => 1, 'fragment_size' => 150],
                    'url' => ['number_of_fragments' => 1, 'fragment_size' => 100],
                    '@id' => ['number_of_fragments' => 1, 'fragment_size' => 100],
                    '*.name' => ['number_of_fragments' => 1, 'fragment_size' => 100],
                    '*.description' => ['number_of_fragments' => 1, 'fragment_size' => 150],
                ]
            ];
        } else {
            $params['body']['query']['bool']['must'][] = ['match_all' => new \stdClass()];
            // Default sort by name if no query
            if ($sort === '_score') {
                $sort = 'name';
                $order = 'asc';
            }
        }

        if (!empty($typeFilter)) {
            $params['body']['query']['bool']['filter'] = [
                ['terms' => ['@type.keyword' => (array) $typeFilter]]
            ];
        }

        if ($sort === 'name') {
            $params['body']['sort'] = [
                ['name.keyword' => ['order' => $order, 'unmapped_type' => 'keyword']],
                ['schema:name.keyword' => ['order' => $order, 'unmapped_type' => 'keyword']]
            ];
        } else {
            $params['body']['sort'] = [
                ['_score' => ['order' => 'desc']]
            ];
        }

        try {
            $response = $this->esClient->search($params);
            $results = $response['hits']['hits'];
            $totalResults = $response['hits']['total']['value'] ?? 0;
            if (isset($response['aggregations']['types']['buckets'])) {
                $facets['types'] = $response['aggregations']['types']['buckets'];
            }
            unset($response);
            gc_collect_cycles();
        } catch (\Exception $e) {
            $this->addFlash('error', 'Elasticsearch error: ' . $e->getMessage());
            $totalResults = 0;
        }

        return $this->render('search/index.html.twig', [
            'query' => $query,
            'results' => $results,
            'totalResults' => $totalResults ?? 0,
            'facets' => $facets,
            'currentSort' => $sort,
            'currentOrder' => $order,
            'currentType' => !empty($typeFilter) ? implode(',', (array) $typeFilter) : '',
            'currentTypes' => (array) $typeFilter,
            'currentPage' => $page,
            'currentLimit' => $limit,
            'totalPages' => (int) ceil($totalResults / $limit),
        ]);
    }

    #[Route('/search/relations', name: 'app_search_relations')]
    public function relations(Request $request): Response
    {
        $name = $request->query->get('name');
        $excludeId = $request->query->get('exclude_id');

        if (empty($name)) {
            return $this->json(['error' => 'Name parameter is required'], 400);
        }

        $params = [
            'index' => $this->esIndex,
            'size' => 50,
            'body' => [
                'query' => [
                    'bool' => [
                        'must' => [
                            'query_string' => [
                                'query' => '"' . $name . '"',
                                'fields' => [
                                    'attendee.name', 'contributor.name', 'organizer.name', 
                                    'performer.name', 'person.name', 'provider.name', 
                                    'creator.name', 'author.name', 'hasCourseInstance.attendee.name', '*.name',
                                    'description', 'text'
                                ],
                                'default_operator' => 'AND'
                            ]
                        ]
                    ]
                ],
                '_source' => ['name', 'schema:name', '@type', '@id', 'url', 'attendee.name', 'contributor.name', 'organizer.name', 'performer.name', 'person.name', 'provider.name', 'creator.name', 'author.name', 'hasCourseInstance.attendee.name']
            ]
        ];

        if (!empty($excludeId)) {
            $params['body']['query']['bool']['must_not'] = [
                ['ids' => ['values' => [$excludeId]]]
            ];
        }

        try {
            $response = $this->esClient->search($params);
            $hits = $response['hits']['hits'];
            unset($response);
            gc_collect_cycles();

            $relations = [];
            foreach ($hits as $hit) {
                $source = $hit['_source'];
                $type = $source['@type'] ?? 'Unknown';
                if (is_array($type)) $type = implode(', ', $type);
                $type = str_replace(['http://schema.org/', 'https://schema.org/'], '', (string)$type);

                $role = 'linked to';
                // Try to find the specific role
                $roles = ['attendee', 'contributor', 'organizer', 'performer', 'person', 'provider', 'creator', 'author'];
                
                // Also check nested roles in hasCourseInstance
                if (isset($source['hasCourseInstance'])) {
                    $instances = is_array($source['hasCourseInstance']) ? $source['hasCourseInstance'] : [$source['hasCourseInstance']];
                    foreach ($instances as $instance) {
                        foreach ($roles as $r) {
                            if (isset($instance[$r])) {
                                $roleData = is_array($instance[$r]) ? $instance[$r] : [$instance[$r]];
                                foreach ($roleData as $rd) {
                                    $checkName = '';
                                    if (is_array($rd) && isset($rd['name'])) {
                                        $checkName = is_array($rd['name']) ? implode(' ', $rd['name']) : (string)$rd['name'];
                                    } elseif (is_string($rd)) {
                                        $checkName = $rd;
                                    } elseif (is_array($rd)) {
                                        $checkName = implode(' ', $rd);
                                    }
        
                                    if (!empty($checkName) && stripos($checkName, $name) !== false) {
                                        $role = $r;
                                        break 3;
                                    }
                                }
                            }
                        }
                    }
                }

                foreach ($roles as $r) {
                    if (isset($source[$r])) {
                        $roleData = is_array($source[$r]) ? $source[$r] : [$source[$r]];
                        foreach ($roleData as $rd) {
                            $checkName = '';
                            if (is_array($rd) && isset($rd['name'])) {
                                $checkName = is_array($rd['name']) ? implode(' ', $rd['name']) : (string)$rd['name'];
                            } elseif (is_string($rd)) {
                                $checkName = $rd;
                            } elseif (is_array($rd)) {
                                $checkName = implode(' ', $rd);
                            }

                            if (!empty($checkName) && stripos($checkName, $name) !== false) {
                                $role = $r;
                                break 2;
                            }
                        }
                    }
                }

                $nameVal = $source['name'] ?? 'Untitled';
                if (is_array($nameVal)) $nameVal = implode(', ', $nameVal);

                $relations[] = [
                    'id' => $hit['_id'],
                    'name' => (string)$nameVal,
                    'type' => (string)$type,
                    'role' => $role,
                    'url' => (is_array($source['@id'] ?? null) ? ($source['@id'][0] ?? '#') : ($source['@id'] ?? $source['url'] ?? '#'))
                ];
            }

            return $this->json([
                'name' => $name,
                'relations' => $relations
            ]);
        } catch (\Exception $e) {
            return $this->json(['error' => $e->getMessage()], 500);
        }
    }
}
