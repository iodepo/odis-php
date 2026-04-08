<?php

namespace App\Command;

use App\Service\OdisCrawler;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(
    name: 'app:odis:index:init',
    description: 'Initialize or recreate the Elasticsearch index with mappings',
)]
class OdisIndexInitCommand extends Command
{
    private OdisCrawler $crawler;

    public function __construct(OdisCrawler $crawler)
    {
        parent::__construct();
        $this->crawler = $crawler;
    }

    protected function configure(): void
    {
        $this
            ->addOption('recreate', null, InputOption::VALUE_NONE, 'Recreate the index if it already exists (deletes all data!)');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $io->title('ODIS Elasticsearch Index Initializer');

        if ($input->getOption('recreate')) {
            if (!$io->confirm('Are you sure you want to RECREATE the index? All data will be lost.', false)) {
                $io->warning('Operation cancelled.');
                return Command::SUCCESS;
            }

            $io->comment('Clearing existing index...');
            try {
                $this->crawler->clearIndex();
            } catch (\Exception $e) {
                $io->error('Failed to clear index: ' . $e->getMessage());
                return Command::FAILURE;
            }
        }

        $io->comment('Creating index and mappings...');
        try {
            $this->crawler->createIndex();
            $io->success('Elasticsearch index initialized successfully.');
        } catch (\Exception $e) {
            if (str_contains($e->getMessage(), 'resource_already_exists_exception')) {
                $io->note('Index already exists. Use --recreate to overwrite it.');
                return Command::SUCCESS;
            }
            $io->error('Failed to create index: ' . $e->getMessage());
            return Command::FAILURE;
        }

        return Command::SUCCESS;
    }
}
