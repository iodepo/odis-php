<?php

namespace App\Twig;

use Twig\Extension\AbstractExtension;
use Twig\TwigFilter;

class TypeLabelExtension extends AbstractExtension
{
    public function getFilters(): array
    {
        return [
            new TwigFilter('clean_type', [$this, 'cleanTypeLabel'])
        ];
    }

    /**
     * Normalize a type label for display:
     * - Strip schema.org prefixes (http/https + schema:)
     * - Remove legacy wrapper artifacts like "{value=...}"
     */
    public function cleanTypeLabel($value): string
    {
        if (is_array($value)) {
            $value = implode(', ', $value);
        }
        $str = (string) $value;
        // Remove wrapper artifacts
        if (str_starts_with($str, '{value=')) {
            $str = substr($str, 7);
            if (str_ends_with($str, '}')) {
                $str = substr($str, 0, -1);
            }
        }
        // Strip schema prefixes
        $str = str_replace([
            'http://schema.org/',
            'https://schema.org/',
            'schema:'
        ], '', $str);
        
        // Add spaces before capitals for better readability (PascalCase to Sentence case)
        // Ensure we don't add space after an existing space or comma-space
        $str = preg_replace('/(?<=[a-z])([A-Z])/', ' $1', $str);
        
        return ucfirst(strtolower(trim($str)));
    }
}
