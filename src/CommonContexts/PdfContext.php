<?php

namespace Proweb\CommonContexts;

use Behatch\Context\BaseContext;
use Behatch\HttpCall\HttpCallResultPool;
use Smalot\PdfParser\Parser;

class PdfContext extends BaseContext
{
    private $pdfs = [];
    private $httpCallResultPool;
    private $parsedTextCached = [];
    private $parsedTextByPageCached = [];
    private $parser;

    public function __construct(HttpCallResultPool $httpCallResultPool)
    {
        $this->httpCallResultPool = $httpCallResultPool;
        $this->parser = new Parser();
    }

    /**
     * @Given I store the last pdf response as :name
     */
    public function iStoreTheLastPdfResponseAs(string $name): void
    {
        $this->pdfs[$name] = $this->httpCallResultPool->getResult()->getValue();
    }

    /**
     * @Given I load the pdf file at path :path as :name
     */
    public function iStoreThePdfFileAtPathAs(string $path, string $name): void
    {
        $this->pdfs[$name] = file_get_contents($this->getFilePathAt($path));
    }

    /**
     * @Then The pdf file :name should contain text :text
     * @Then The pdf file :name should contain text :text in page :page
     */
    public function thePdfFileShouldContainText(string $name, string $text, ?int $page = null): void
    {
        $this->preparePdfFile($name);

        $pageContent = null !== $page ? $this->parsedTextByPageCached[$name][$page] : $this->parsedTextCached[$name];

        if (false === strpos($pageContent, $this->cleanText($text))) {
            throw new \LogicException(sprintf('Pdf file "%s" should contain text "%s"', $name, $text));
        }
    }

    /**
     * @Then The pdf file :name should have :number page(s)
     */
    public function thePdfFileShouldHavePages(string $name, int $number): void
    {
        $this->preparePdfFile($name);

        $numberOfPages = \count($this->parsedTextByPageCached[$name] ?? []);

        if ($number !== $numberOfPages) {
            throw new \LogicException(sprintf('Pdf file "%s" should contain "%d" pages, found "%d"', $name, $number, $numberOfPages));
        }
    }

    /**
     * @Then The pdf file :name should not contain text :text
     * @Then The pdf file :name should not contain text :text in page :page
     */
    public function thePdfFileShouldNotContainText(string $name, string $text, ?int $page = null): void
    {
        $this->preparePdfFile($name);

        $pageContent = null !== $page ? $this->parsedTextByPageCached[$name][$name] : $this->parsedTextCached[$name];

        if (false !== strpos($pageContent, $this->cleanText($text))) {
            throw new \LogicException(sprintf('Pdf file "%s" should not contain text "%s"', $name, $text));
        }
    }

    private function preparePdfFile(string $name): void
    {
        if (isset($this->parsedTextCached[$name])) {
            return;
        }

        $pdf = $this->parser->parseContent($this->pdfs[$name]);
        $this->parsedTextCached[$name] = $this->cleanText($pdf->getText());
        $this->parsedTextByPageCached[$name] = [];

        foreach ($pdf->getPages() as $index => $page) {
            $this->parsedTextByPageCached[$name][$index + 1] = $this->cleanText($page->getText());
        }
    }

    private function cleanText(string $text): string
    {
        return preg_replace(
            [
                '/\s/',
                '/&#39;/',
            ],
            [
                '',
                '\'',
            ],
            $text
        );
    }

    private function getFilePathAt(string $path): string
    {
        $files = glob($path);

        if (empty($files)) {
            throw new \LogicException(sprintf('No files found for "%s".', $path));
        }

        return $files[0];
    }
}
