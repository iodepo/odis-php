<?php

namespace App\Tests\Controller;

use App\Entity\CrawlStat;
use App\Repository\CrawlStatRepository;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

class DashboardControllerTest extends WebTestCase
{
    public function testHistoryShowsTargetedCrawls(): void
    {
        $client = static::createClient();
        $container = $client->getContainer();
        $em = $container->get('doctrine')->getManager();

        // Clear existing stats
        $em->createQuery('DELETE FROM App\Entity\CrawlStat')->execute();

        // Create a targeted stat
        $stat = new CrawlStat();
        $stat->setType('targeted');
        $stat->setStatus('completed');
        $stat->setNodesFound(5);
        $stat->setPagesCrawled(10);
        $stat->setValidJsonLds(3);
        $stat->setCommandLine('php bin/console app:odis:crawl 123');
        $em->persist($stat);

        // Create a full stat
        $stat2 = new CrawlStat();
        $stat2->setType('full');
        $stat2->setStatus('completed');
        $stat2->setNodesFound(100);
        $stat2->setPagesCrawled(500);
        $stat2->setValidJsonLds(200);
        $stat2->setCommandLine('php bin/console app:odis:crawl');
        $em->persist($stat2);

        $em->flush();

        // Check Dashboard
        $crawler = $client->request('GET', '/dashboard');
        $this->assertResponseIsSuccessful();
        
        // Should see 2 rows in history table
        // The history table body has id="history-table-body" in the template (I should check that)
        // From my previous 'open' call: <tbody id="history-table-body">
        $this->assertCount(2, $crawler->filter('#history-table-body tr:not(.collapse)'));
        
        // Check API
        $client->request('GET', '/api/dashboard/status');
        $this->assertResponseIsSuccessful();
        $data = json_decode($client->getResponse()->getContent(), true);
        $this->assertCount(2, $data['history']);
    }
}
