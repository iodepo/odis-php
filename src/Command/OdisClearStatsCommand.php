<?php

namespace App\Command;

use App\Repository\CrawlStatRepository;
use App\Service\OdisCrawler;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(
    name: 'app:odis:clear-stats',
    description: 'Clear all crawl statistics and report data',
)]
class OdisClearStatsCommand extends Command
{
    private CrawlStatRepository $repository;
    private OdisCrawler $crawler;

    public function __construct(CrawlStatRepository $repository, OdisCrawler $crawler)
    {
        parent::__construct();
        $this->repository = $repository;
        $this->crawler = $crawler;
    }

    protected function configure(): void
    {
        $this
            ->addOption('all', 'a', InputOption::VALUE_NONE, 'Clear both crawl statistics and Elasticsearch index');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        if ($input->getOption('all')) {
            $io->warning('Clearing BOTH crawl statistics and Elasticsearch index...');
            $this->crawler->clearIndex();
            $count = $this->repository->clearStats();
            $io->success(sprintf('Cleared %d statistic records and emptied the search index.', $count));
        } else {
            $io->note('Clearing crawl statistics and report data...');
            $count = $this->repository->clearStats();
            $io->success(sprintf('Cleared %d statistic records. Search index was NOT affected.', $count));
        }

        return Command::SUCCESS;
    }
}
