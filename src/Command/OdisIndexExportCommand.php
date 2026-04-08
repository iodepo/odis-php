<?php

namespace App\Command;

use App\Service\OdisCrawler;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(
    name: 'app:odis:index:export',
    description: 'Export Elasticsearch index mappings as JSON',
)]
class OdisIndexExportCommand extends Command
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
            ->addArgument('filename', InputArgument::OPTIONAL, 'Filename to save the JSON mapping to');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $mapping = $this->crawler->getIndexMapping();
        $json = json_encode(['mappings' => $mapping], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);

        $filename = $input->getArgument('filename');
        if ($filename) {
            if (file_put_contents($filename, $json)) {
                $io->success(sprintf('Mappings exported to %s', $filename));
            } else {
                $io->error(sprintf('Failed to write to file: %s', $filename));
                return Command::FAILURE;
            }
        } else {
            $output->writeln($json);
        }

        return Command::SUCCESS;
    }
}
