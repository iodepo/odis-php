<?php

namespace App\Tests\Template;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Finder\Finder;

class FontAwesomeTest extends TestCase
{
    public function testNoFa4IconsUsed(): void
    {
        $projectDir = dirname(__DIR__, 2);
        $templatesDir = $projectDir . '/templates';

        $finder = new Finder();
        $finder->files()->in($templatesDir)->name('*.html.twig');

        $fa4Pattern = '/(class|processedHtml \+=) ?[=+] ?["\'].*fa fa-.*["\']/';
        $foundMatches = [];

        foreach ($finder as $file) {
            $content = $file->getContents();
            if (preg_match_all($fa4Pattern, $content, $matches)) {
                $foundMatches[$file->getRelativePathname()] = $matches[0];
            }
        }

        $this->assertEmpty($foundMatches, "Found legacy Font Awesome 4 icon classes (fa fa-) in the following files: " . json_encode($foundMatches, JSON_PRETTY_PRINT));
    }

    public function testNoFa4LinkInBase(): void
    {
        $projectDir = dirname(__DIR__, 2);
        $baseTemplate = $projectDir . '/templates/base.html.twig';
        
        $this->assertFileExists($baseTemplate);
        
        $content = file_get_contents($baseTemplate);
        $this->assertStringNotContainsString('font-awesome/4.7.0/css/font-awesome.min.css', $content, "The legacy Font Awesome 4 stylesheet is still included in base.html.twig");
    }
}
