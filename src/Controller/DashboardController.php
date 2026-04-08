<?php

namespace App\Controller;

use Elastic\Transport\Exception\NoNodeAvailableException;
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
        $history = $repository->findBy([], ['id' => 'DESC'], 100);

        // Fetch cumulative counts from Elasticsearch and DB
        $totalValid = 0;
        $esError = null;
        try {
            $response = $esClient->count(['index' => 'odis_metadata']);
            $totalValid = $response['count'];
        } catch (NoNodeAvailableException $e) {
            $esError = 'Elasticsearch is unreachable. Please check your ELASTICSEARCH_URL configuration and ensure the service is running.';
        } catch (\Exception $e) {
            $esError = 'An error occurred while connecting to Elasticsearch: ' . $e->getMessage();
        }

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
            'esError' => $esError,
        ]);
    }

    #[Route('/dashboard/clear-stats', name: 'app_dashboard_clear_stats', methods: ['POST'])]
    public function clearStats(CrawlStatRepository $repository): Response
    {
        $repository->clearStats();
        $this->addFlash('success', 'All crawl statistics and reports have been cleared.');
        return $this->redirectToRoute('app_dashboard');
    }
}
