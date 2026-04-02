<?php

namespace App\Command;

use App\Service\OdisCrawler;
use App\Entity\CrawlStat;
use Doctrine\ORM\EntityManagerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Process\Process;

#[AsCommand(
    name: 'app:odis:crawl',
    description: 'Crawl ODIS metadata and index it into Elasticsearch',
)]
class OdisCrawlCommand extends Command
{
    private OdisCrawler $crawler;
    private EntityManagerInterface $entityManager;

    public function __construct(OdisCrawler $crawler, EntityManagerInterface $entityManager)
    {
        parent::__construct();
        $this->crawler = $crawler;
        $this->entityManager = $entityManager;
    }

    protected function configure(): void
    {
        $this
            ->addArgument('ids', InputArgument::IS_ARRAY, 'Optional list of ODISCat IDs to crawl')
            ->addOption('skip', 's', InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'List of ODISCat IDs to skip')
            ->addOption('parallel', 'p', InputOption::VALUE_NONE, 'Run crawl in parallel sessions for each datasource')
            ->addOption('concurrency', 'c', InputOption::VALUE_REQUIRED, 'Max concurrent processes for parallel crawl', 5)
            ->addOption('limit', 'l', InputOption::VALUE_REQUIRED, 'Limit the number of records crawled per datasource', 0);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        ini_set('memory_limit', '512M');
        $io = new SymfonyStyle($input, $output);
        $io->title('ODIS Metadata Crawler');

        $commandLine = 'php bin/console ' . $this->getName();
        foreach ($_SERVER['argv'] as $i => $arg) {
            if ($i === 0 || $arg === 'bin/console' || $arg === $this->getName()) continue;
            $commandLine .= ' ' . (str_contains($arg, ' ') ? '"' . $arg . '"' : $arg);
        }
        $this->crawler->setCommandLine($commandLine);

        if ($output->getVerbosity() >= OutputInterface::VERBOSITY_VERBOSE) {
            $this->crawler->setOutput($output);
        }

        $limit = (int) $input->getOption('limit');
        if ($limit > 0) {
            $this->crawler->setLimit($limit);
        }

        $ids = $this->parseIds($input->getArgument('ids'));
        $skipIds = $this->parseIds($input->getOption('skip'));

        if ($input->getOption('parallel')) {
            return $this->executeParallel($input, $output, $ids, $skipIds);
        }

        $this->crawler->run($ids, $skipIds);

        $io->success('Crawl completed.');

        return Command::SUCCESS;
    }

    private function executeParallel(InputInterface $input, OutputInterface $output, array $ids, array $skipIds): int
    {
        $io = new SymfonyStyle($input, $output);
        $concurrency = (int) $input->getOption('concurrency');
        $limit = (int) $input->getOption('limit');
        
        $commandLine = 'php bin/console ' . $this->getName();
        foreach ($_SERVER['argv'] as $i => $arg) {
            if ($i === 0 || $arg === 'bin/console' || $arg === $this->getName()) continue;
            $commandLine .= ' ' . (str_contains($arg, ' ') ? '"' . $arg . '"' : $arg);
        }

        if (empty($ids)) {
            $io->comment('Fetching all records for parallel crawl...');
            $records = $this->crawler->getRecords();
            $ids = array_keys($records);
        }

        $ids = array_diff($ids, $skipIds);
        $total = count($ids);

        if ($total === 0) {
            $io->warning('No IDs to crawl.');
            return Command::SUCCESS;
        }

        $io->note(sprintf('Starting parallel crawl for %d datasource(s) with concurrency %d', $total, $concurrency));

        $masterStat = new CrawlStat();
        $masterStat->setType('parallel_master');
        $masterStat->setStatus('in_progress');
        $masterStat->setCommandLine($commandLine);
        $masterStat->setNodesFound($total);
        $this->entityManager->persist($masterStat);
        $this->entityManager->flush();

        $processes = [];
        $idsToCrawl = array_values($ids);
        $currentIndex = 0;
        $completed = 0;

        $progressBar = $io->createProgressBar($total);
        $progressBar->start();

        while ($completed < $total) {
            // Fill up processes to concurrency limit
            while (count($processes) < $concurrency && $currentIndex < $total) {
                $id = $idsToCrawl[$currentIndex++];
                // Build command: php bin/console app:odis:crawl <id>
                // We don't pass --parallel to child processes to avoid recursion
                $args = [PHP_BINARY, '-d', 'memory_limit=512M', 'bin/console', 'app:odis:crawl', $id];
                if ($limit > 0) {
                    $args[] = '--limit';
                    $args[] = (string) $limit;
                }
                $process = new Process($args);
                $process->start();
                $processes[$id] = $process;
            }

            // Check for finished processes
            foreach ($processes as $id => $process) {
                if (!$process->isRunning()) {
                    $completed++;
                    $progressBar->advance();
                    
                    if (!$process->isSuccessful()) {
                        $io->error(sprintf('Process for ID %s failed: %s', $id, $process->getErrorOutput()));
                        $masterStat->setCrawlerErrors($masterStat->getCrawlerErrors() + 1);
                        $masterStat->addErrorDetail("ID $id failed: " . substr($process->getErrorOutput(), 0, 200));
                    }
                    
                    unset($processes[$id]);
                    $this->entityManager->flush();
                }
            }

            usleep(100000); // 100ms
        }

        $progressBar->finish();
        $masterStat->setStatus('completed');
        $masterStat->setFinishedAt(new \DateTimeImmutable());
        $this->entityManager->flush();
        
        $io->newLine(2);
        $io->success('Parallel crawl completed.');

        return Command::SUCCESS;
    }

    private function parseIds(array $ids): array
    {
        $result = [];
        foreach ($ids as $id) {
            if (str_contains($id, ',')) {
                $parts = explode(',', $id);
                foreach ($parts as $part) {
                    $part = trim($part);
                    if ($part !== '') {
                        $result[] = $part;
                    }
                }
            } else {
                $id = trim($id);
                if ($id !== '') {
                    $result[] = $id;
                }
            }
        }
        return array_unique($result);
    }
}
