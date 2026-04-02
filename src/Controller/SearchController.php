<?php

namespace App\Controller;

use Elastic\Elasticsearch\Client;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Annotation\Route;

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
        $query = $request->query->get('q', '');
        $sort = $request->query->get('sort', '_score');
        $order = $request->query->get('order', 'desc');
        $typeFilter = $request->query->get('type', '');
        $page = max(1, (int) $request->query->get('page', 1));
        $limit = max(1, min(100, (int) $request->query->get('limit', 20)));
        
        $results = [];
        $facets = [];
        $totalResults = 0;

        if (!empty($query)) {
            $params = [
                'index' => $this->esIndex,
                'from'  => ($page - 1) * $limit,
                'size'  => $limit,
                'body'  => [
                    'query' => [
                        'bool' => [
                            'must' => [
                                'query_string' => [
                                    'query'  => '*' . $query . '*',
                                    'fields' => ['name^3', 'description', 'keywords^2', 'text', 'url', '@id', 'attendee.*', 'contributor.*', 'organizer.*', 'performer.*', 'person.*', 'provider.*', 'creator.*', 'author.*', '*.name', '*.description'],
                                    'default_operator' => 'AND',
                                    'allow_leading_wildcard' => true
                                ]
                            ]
                        ]
                    ],
                    'highlight' => [
                        'fields' => [
                            'name' => new \stdClass(),
                            'description' => new \stdClass(),
                            'keywords' => new \stdClass(),
                            'text' => new \stdClass()
                        ]
                    ],
                    'aggs' => [
                        'types' => [
                            'terms' => ['field' => '@type.keyword', 'size' => 10]
                        ]
                    ]
                ]
            ];

            if (!empty($typeFilter)) {
                $params['body']['query']['bool']['filter'] = [
                    ['term' => ['@type.keyword' => $typeFilter]]
                ];
            }

            if ($sort === 'name') {
                $params['body']['sort'] = [
                    ['name.keyword' => ['order' => $order]]
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
            } catch (\Exception $e) {
                $this->addFlash('error', 'Elasticsearch error: ' . $e->getMessage());
                $totalResults = 0;
            }
        }

        return $this->render('search/index.html.twig', [
            'query' => $query,
            'results' => $results,
            'totalResults' => $totalResults ?? 0,
            'facets' => $facets,
            'currentSort' => $sort,
            'currentOrder' => $order,
            'currentType' => $typeFilter,
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
            'size' => 100,
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
                '_source' => ['name', '@type', '@id', 'url', 'attendee', 'contributor', 'organizer', 'performer', 'person', 'provider', 'creator', 'author', 'hasCourseInstance']
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

            // Fallback: if no exact name matches, try a broader search without quotes
            if (empty($hits) && !empty($name)) {
                $params['body']['query']['bool']['must']['query_string']['query'] = $name;
                $response = $this->esClient->search($params);
                $hits = $response['hits']['hits'];
            }
            
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
