<?php

namespace App\Controller;

use App\Repository\CrawlStatRepository;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\HeaderUtils;
use Symfony\Component\Routing\Attribute\Route;

class DownloadController extends AbstractController
{
    #[Route('/download/invalid', name: 'app_download_invalid')]
    public function downloadInvalid(CrawlStatRepository $repository): Response
    {
        $allErrors = $repository->getAllErrorDetails();
        
        // Filter for invalid JSON-LD entries
        $invalidEntries = array_filter($allErrors, function($error) {
            return str_contains($error['message'], 'Invalid JSON-LD format') || str_contains($error['message'], 'No JSON-LD found');
        });

        return $this->generateCsvResponse($invalidEntries, 'invalid_jsonld_report.csv');
    }

    #[Route('/download/errors', name: 'app_download_errors')]
    public function downloadErrors(CrawlStatRepository $repository): Response
    {
        $allErrors = $repository->getAllErrorDetails();
        
        // Filter for actual crawler/network/system errors
        $errorEntries = array_filter($allErrors, function($error) {
            return !str_contains($error['message'], 'Invalid JSON-LD format') && !str_contains($error['message'], 'No JSON-LD found');
        });

        return $this->generateCsvResponse($errorEntries, 'crawler_errors_report.csv');
    }

    private function generateCsvResponse(array $data, string $filename): Response
    {
        $handle = fopen('php://memory', 'r+');
        fputcsv($handle, ['Crawl ID', 'Date', 'Type', 'Message']);

        foreach ($data as $row) {
            fputcsv($handle, [
                $row['crawl_id'],
                $row['date']->format('Y-m-d H:i:s'),
                $row['crawl_type'],
                $row['message']
            ]);
        }

        rewind($handle);
        $content = stream_get_contents($handle);
        fclose($handle);

        $response = new Response($content);
        $disposition = HeaderUtils::makeDisposition(
            HeaderUtils::DISPOSITION_ATTACHMENT,
            $filename
        );

        $response->headers->set('Content-Type', 'text/csv');
        $response->headers->set('Content-Disposition', $disposition);

        return $response;
    }
}
