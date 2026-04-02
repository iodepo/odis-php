<?php

namespace App\Controller;

use App\Repository\CrawlStatRepository;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\Routing\Annotation\Route;

class DashboardApiController extends AbstractController
{
    #[Route('/api/dashboard/status', name: 'app_api_dashboard_status')]
    public function status(CrawlStatRepository $repository, \Elastic\Elasticsearch\Client $esClient): JsonResponse
    {
        // To avoid "swapping" numbers if multiple crawls run, we prefer one that is actively in progress
        // or the most recent completed one.
        $inProgress = $repository->findBy(['status' => 'in_progress'], ['id' => 'DESC'], 1);
        $latestStat = !empty($inProgress) ? $inProgress[0] : $repository->findLatest();

        $history = $repository->findBy([], ['id' => 'DESC'], 10);

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

        $historyData = [];
        foreach ($history as $stat) {
            $total = $stat->getValidJsonLds() + $stat->getInvalidJsonLds();
            $successRate = $total > 0 ? round(($stat->getValidJsonLds() / $total) * 100, 1) : 0;
            
            $historyData[] = [
                'id' => $stat->getId(),
                'createdAt' => $stat->getCreatedAt()->format('Y-m-d H:i:s'),
                'finishedAt' => $stat->getFinishedAt() ? $stat->getFinishedAt()->format('Y-m-d H:i:s') : null,
                'duration' => $this->formatDuration($stat->getCreatedAt(), $stat->getFinishedAt()),
                'nodesFound' => $stat->getNodesFound(),
                'pagesCrawled' => $stat->getPagesCrawled(),
                'validJsonLds' => $stat->getValidJsonLds(),
                'invalidJsonLds' => $stat->getInvalidJsonLds(),
                'crawlerErrors' => $stat->getCrawlerErrors(),
                'status' => $stat->getStatus(),
                'successRate' => $successRate . '%',
                'hasErrors' => !empty($stat->getErrorDetails()),
                'errorDetails' => $stat->getErrorDetails(),
                'commandLine' => $stat->getCommandLine(),
            ];
        }

        return new JsonResponse([
            'totalValid' => $totalValid,
            'totalNodes' => $totalNodes,
            'totalPages' => $totalPages,
            'totalInvalid' => $totalInvalid,
            'totalIssues' => $totalIssues,
            'processedEntries' => $processedEntries,
            'latest' => $latestStat ? [
                'id' => $latestStat->getId(),
                'nodesFound' => $latestStat->getNodesFound(),
                'pagesCrawled' => $latestStat->getPagesCrawled(),
                'validJsonLds' => $latestStat->getValidJsonLds(),
                'invalidJsonLds' => $latestStat->getInvalidJsonLds(),
                'crawlerErrors' => $latestStat->getCrawlerErrors(),
                'errorCount' => count($latestStat->getErrorDetails() ?? []),
                'status' => $latestStat->getStatus(),
                'createdAt' => $latestStat->getCreatedAt()->format('Y-m-d H:i:s'),
                'duration' => $this->formatDuration($latestStat->getCreatedAt(), $latestStat->getFinishedAt()),
            ] : null,
            'history' => $historyData
        ]);
    }

    private function formatDuration(\DateTimeImmutable $start, ?\DateTimeImmutable $end): string
    {
        $end = $end ?? new \DateTimeImmutable();
        $diff = $start->diff($end);

        $parts = [];
        if ($diff->h > 0) $parts[] = $diff->h . 'h';
        if ($diff->i > 0) $parts[] = $diff->i . 'm';
        if ($diff->s > 0 || empty($parts)) $parts[] = $diff->s . 's';

        return implode(' ', $parts);
    }
}
