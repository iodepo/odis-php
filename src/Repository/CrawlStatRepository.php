<?php

namespace App\Repository;

use App\Entity\CrawlStat;
use Doctrine\Bundle\DoctrineBundle\Repository\ServiceEntityRepository;
use Doctrine\Persistence\ManagerRegistry;

/**
 * @extends ServiceEntityRepository<CrawlStat>
 */
class CrawlStatRepository extends ServiceEntityRepository
{
    public function __construct(ManagerRegistry $registry)
    {
        parent::__construct($registry, CrawlStat::class);
    }

    public function findLatest(): ?CrawlStat
    {
        return $this->findOneBy([], ['id' => 'DESC']);
    }

    public function countUniqueNodesFound(): int
    {
        // Prioritize full crawls or parallel masters
        $max = (int) $this->createQueryBuilder('s')
            ->select('MAX(s.nodesFound)')
            ->where('s.type = :full OR s.type = :master')
            ->setParameter('full', 'full')
            ->setParameter('master', 'parallel_master')
            ->getQuery()
            ->getSingleScalarResult();
        
        if ($max > 0) {
            return $max;
        }

        // Fallback to any record if no full/master records found
        $max = (int) $this->createQueryBuilder('s')
            ->select('MAX(s.nodesFound)')
            ->getQuery()
            ->getSingleScalarResult();
        
        return $max ?: 0;
    }

    public function sumTotalPagesCrawled(): int
    {
        // Sum pages from 'targeted' crawls (individual sub-processes)
        // and 'full' crawls (single-process full crawl).
        // We exclude 'parallel_master' to avoid double counting if it ever logs pages.
        return (int) $this->createQueryBuilder('s')
            ->select('SUM(s.pagesCrawled)')
            ->where('s.type = :targeted OR s.type = :full')
            ->setParameter('targeted', 'targeted')
            ->setParameter('full', 'full')
            ->getQuery()
            ->getSingleScalarResult();
    }

    public function sumTotalInvalidJsonLds(): int
    {
        return (int) $this->createQueryBuilder('s')
            ->select('SUM(s.invalidJsonLds)')
            ->where('s.type = :targeted OR s.type = :full')
            ->setParameter('targeted', 'targeted')
            ->setParameter('full', 'full')
            ->getQuery()
            ->getSingleScalarResult();
    }
    public function sumTotalErrors(): int
    {
        // This is a bit tricky because errorDetails is a JSON array.
        // We want the total count of issues across all targeted and full crawls.
        // For simplicity in SQLite/Doctrine, we might just sum crawlerErrors if it exists,
        // but crawlerErrors currently seems to be used for process failures in parallel master.
        
        // Let's count all entries in errorDetails across relevant stats.
        $stats = $this->createQueryBuilder('s')
            ->select('s.errorDetails')
            ->where('s.type = :targeted OR s.type = :full')
            ->setParameter('targeted', 'targeted')
            ->setParameter('full', 'full')
            ->getQuery()
            ->getResult();
            
        $total = 0;
        foreach ($stats as $stat) {
            if (isset($stat['errorDetails']) && is_array($stat['errorDetails'])) {
                $total += count($stat['errorDetails']);
            }
        }
        
        return $total;
    }

    /**
     * @return array<array{id: int, createdAt: \DateTimeImmutable, message: string, type: string}>
     */
    public function getAllErrorDetails(): array
    {
        $stats = $this->createQueryBuilder('s')
            ->select('s.id, s.createdAt, s.errorDetails, s.type')
            ->where('s.type = :targeted OR s.type = :full OR s.type = :master')
            ->setParameter('targeted', 'targeted')
            ->setParameter('full', 'full')
            ->setParameter('master', 'parallel_master')
            ->getQuery()
            ->getResult();

        $allErrors = [];
        foreach ($stats as $stat) {
            if (isset($stat['errorDetails']) && is_array($stat['errorDetails'])) {
                foreach ($stat['errorDetails'] as $error) {
                    $id = null;
                    $message = $error;
                    
                    if (is_array($error)) {
                        $id = $error['id'] ?? null;
                        $message = $error['message'] ?? '';
                    }

                    $allErrors[] = [
                        'crawl_id' => $stat['id'],
                        'date' => $stat['createdAt'],
                        'message' => $message,
                        'datasource_id' => $id,
                        'crawl_type' => $stat['type']
                    ];
                }
            }
        }

        return $allErrors;
    }

    /**
     * @return array<array{id: int, name: string}>
     */
    public function getAllProcessedEntries(): array
    {
        $stats = $this->createQueryBuilder('s')
            ->select('s.processedEntries')
            ->where('s.type = :full OR s.type = :master OR s.type = :targeted')
            ->setParameter('full', 'full')
            ->setParameter('master', 'parallel_master')
            ->setParameter('targeted', 'targeted')
            ->getQuery()
            ->getResult();

        $allEntries = [];
        $seenIds = [];
        foreach ($stats as $stat) {
            if (isset($stat['processedEntries']) && is_array($stat['processedEntries'])) {
                foreach ($stat['processedEntries'] as $entry) {
                    if (isset($entry['id']) && !in_array($entry['id'], $seenIds)) {
                        $allEntries[] = $entry;
                        $seenIds[] = $entry['id'];
                    }
                }
            }
        }

        return $allEntries;
    }

    public function clearStats(): int
    {
        return $this->createQueryBuilder('s')
            ->delete()
            ->getQuery()
            ->execute();
    }
}
