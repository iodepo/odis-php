<?php

namespace App\Controller;

use App\Repository\CrawlStatRepository;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

class DashboardController extends AbstractController
{
    #[Route('/dashboard', name: 'app_dashboard')]
    public function index(CrawlStatRepository $repository, \Elastic\Elasticsearch\Client $esClient): Response
    {
        $latestStat = $repository->findLatest();
        $history = $repository->findBy([], ['createdAt' => 'DESC'], 10);

        // Fetch cumulative counts from Elasticsearch and DB
        $totalValid = 0;
        try {
            $response = $esClient->count(['index' => 'odis_metadata']);
            $totalValid = $response['count'];
        } catch (\Exception $e) {}

        // Calculate cumulative metrics
        $totalNodes = $repository->countUniqueNodesFound();
        $totalPages = $repository->sumTotalPagesCrawled();
        $totalInvalid = $repository->sumTotalInvalidJsonLds();
        $totalIssues = $repository->sumTotalErrors();
        $processedEntries = $repository->getAllProcessedEntries();

        return $this->render('dashboard/index.html.twig', [
            'latestStat' => $latestStat,
            'history' => $history,
            'totalValid' => $totalValid,
            'totalNodes' => $totalNodes,
            'totalPages' => $totalPages,
            'totalInvalid' => $totalInvalid,
            'totalIssues' => $totalIssues,
            'processedEntries' => $processedEntries,
        ]);
    }
}
