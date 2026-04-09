<?php

namespace App\Tests\Twig;

use App\Twig\TypeLabelExtension;
use PHPUnit\Framework\TestCase;

class TypeLabelExtensionTest extends TestCase
{
    public function testCleanTypeLabelStripsSchemaPrefixesAndWrappers()
    {
        $ext = new TypeLabelExtension();

        $this->assertSame('Dataset', $ext->cleanTypeLabel('https://schema.org/Dataset'));
        $this->assertSame('Dataset', $ext->cleanTypeLabel('http://schema.org/Dataset'));
        $this->assertSame('Dataset', $ext->cleanTypeLabel('schema:Dataset'));
        $this->assertSame('Dataset', $ext->cleanTypeLabel('{value=Dataset}'));
        $this->assertSame('Dataset, service', $ext->cleanTypeLabel(['schema:Dataset', 'schema:Service']));
        $this->assertSame('Dataset', $ext->cleanTypeLabel('dataset'));
        $this->assertSame('Dataset', $ext->cleanTypeLabel('DATASET'));
        $this->assertSame('Research project', $ext->cleanTypeLabel('ResearchProject'));
    }
}
