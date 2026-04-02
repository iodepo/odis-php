<?php

namespace App\Entity;

use App\Repository\CrawlStatRepository;
use Doctrine\ORM\Mapping as ORM;

#[ORM\Entity(repositoryClass: CrawlStatRepository::class)]
class CrawlStat
{
    #[ORM\Id]
    #[ORM\GeneratedValue]
    #[ORM\Column]
    private ?int $id = null;

    #[ORM\Column]
    private int $nodesFound = 0;

    #[ORM\Column]
    private int $pagesCrawled = 0;

    #[ORM\Column]
    private int $validJsonLds = 0;

    #[ORM\Column]
    private int $invalidJsonLds = 0;

    #[ORM\Column(options: ["default" => 0])]
    private int $crawlerErrors = 0;

    #[ORM\Column(type: "json", nullable: true)]
    private ?array $errorDetails = [];

    #[ORM\Column(length: 20, options: ["default" => "completed"])]
    private string $status = 'completed';

    #[ORM\Column(length: 20, nullable: true)]
    private ?string $type = null;

    #[ORM\Column(type: "text", nullable: true)]
    private ?string $commandLine = null;

    #[ORM\Column]
    private \DateTimeImmutable $createdAt;

    #[ORM\Column(nullable: true)]
    private ?\DateTimeImmutable $finishedAt = null;

    #[ORM\Column(type: "json", nullable: true)]
    private ?array $processedEntries = [];

    private ?int $currentEntryId = null;

    public function __construct()
    {
        $this->createdAt = new \DateTimeImmutable();
    }

    public function getProcessedEntries(): ?array
    {
        return $this->processedEntries;
    }

    public function setProcessedEntries(?array $processedEntries): self
    {
        $this->processedEntries = $processedEntries;
        return $this;
    }

    public function addProcessedEntry(int $id, string $name): self
    {
        if ($this->processedEntries === null) {
            $this->processedEntries = [];
        }
        
        $this->currentEntryId = $id;

        // Avoid duplicates in the same crawl session
        foreach ($this->processedEntries as &$entry) {
            if ($entry['id'] === $id) {
                return $this;
            }
        }

        $this->processedEntries[] = [
            'id' => $id,
            'name' => $name,
            'processedAt' => (new \DateTimeImmutable())->format(\DateTimeInterface::ATOM),
            'recordsFound' => 0,
            'validJsonLds' => 0,
            'errorsCount' => 0,
            'errors' => []
        ];
        return $this;
    }

    public function incrementEntryRecordsFound(): self
    {
        if ($this->currentEntryId === null || $this->processedEntries === null) {
            return $this;
        }

        foreach ($this->processedEntries as &$entry) {
            if ($entry['id'] === $this->currentEntryId) {
                $entry['recordsFound'] = ($entry['recordsFound'] ?? 0) + 1;
                break;
            }
        }
        return $this;
    }

    public function incrementEntryValidJsonLds(): self
    {
        if ($this->currentEntryId === null || $this->processedEntries === null) {
            return $this;
        }

        foreach ($this->processedEntries as &$entry) {
            if ($entry['id'] === $this->currentEntryId) {
                $entry['validJsonLds'] = ($entry['validJsonLds'] ?? 0) + 1;
                break;
            }
        }
        return $this;
    }

    public function incrementEntryErrorsCount(): self
    {
        if ($this->currentEntryId === null || $this->processedEntries === null) {
            return $this;
        }

        foreach ($this->processedEntries as &$entry) {
            if ($entry['id'] === $this->currentEntryId) {
                $entry['errorsCount'] = ($entry['errorsCount'] ?? 0) + 1;
                break;
            }
        }
        return $this;
    }

    public function addEntryError(string $message): self
    {
        if ($this->currentEntryId === null || $this->processedEntries === null) {
            return $this;
        }

        foreach ($this->processedEntries as &$entry) {
            if ($entry['id'] === $this->currentEntryId) {
                if (!isset($entry['errors'])) {
                    $entry['errors'] = [];
                }
                // Only keep first few unique errors to avoid bloat
                if (count($entry['errors']) < 5 && !in_array($message, $entry['errors'])) {
                    $entry['errors'][] = $message;
                }
                break;
            }
        }
        return $this;
    }

    public function getId(): ?int
    {
        return $this->id;
    }

    public function getNodesFound(): int
    {
        return $this->nodesFound;
    }

    public function setNodesFound(int $nodesFound): self
    {
        $this->nodesFound = $nodesFound;
        return $this;
    }

    public function getPagesCrawled(): int
    {
        return $this->pagesCrawled;
    }

    public function setPagesCrawled(int $pagesCrawled): self
    {
        $this->pagesCrawled = $pagesCrawled;
        return $this;
    }

    public function getValidJsonLds(): int
    {
        return $this->validJsonLds;
    }

    public function setValidJsonLds(int $validJsonLds): self
    {
        $this->validJsonLds = $validJsonLds;
        return $this;
    }

    public function getInvalidJsonLds(): int
    {
        return $this->invalidJsonLds;
    }

    public function setInvalidJsonLds(int $invalidJsonLds): self
    {
        $this->invalidJsonLds = $invalidJsonLds;
        return $this;
    }

    public function getCrawlerErrors(): int
    {
        return $this->crawlerErrors;
    }

    public function setCrawlerErrors(int $crawlerErrors): self
    {
        $this->crawlerErrors = $crawlerErrors;
        return $this;
    }

    public function getCreatedAt(): \DateTimeImmutable
    {
        return $this->createdAt;
    }

    public function setCreatedAt(\DateTimeImmutable $createdAt): self
    {
        $this->createdAt = $createdAt;
        return $this;
    }

    public function getFinishedAt(): ?\DateTimeImmutable
    {
        return $this->finishedAt;
    }

    public function setFinishedAt(?\DateTimeImmutable $finishedAt): self
    {
        $this->finishedAt = $finishedAt;
        return $this;
    }

    public function getErrorDetails(): ?array
    {
        return $this->errorDetails;
    }

    public function setErrorDetails(?array $errorDetails): self
    {
        $this->errorDetails = $errorDetails;
        return $this;
    }

    public function getStatus(): string
    {
        return $this->status;
    }

    public function setStatus(string $status): self
    {
        $this->status = $status;
        return $this;
    }

    public function getType(): ?string
    {
        return $this->type;
    }

    public function setType(?string $type): self
    {
        $this->type = $type;
        return $this;
    }

    public function getCommandLine(): ?string
    {
        return $this->commandLine;
    }

    public function setCommandLine(?string $commandLine): self
    {
        $this->commandLine = $commandLine;
        return $this;
    }

    public function addErrorDetail(string $message): self
    {
        if ($this->errorDetails === null) {
            $this->errorDetails = [];
        }
        $this->errorDetails[] = $message;
        return $this;
    }
}
