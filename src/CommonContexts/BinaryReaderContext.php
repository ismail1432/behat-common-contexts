<?php

use Behat\Gherkin\Node\PyStringNode;
use Behat\Gherkin\Node\TableNode;
use Behat\MinkExtension\Context\RawMinkContext;
use Coduo\PHPMatcher\PHPMatcher;
use PhpOffice\PhpSpreadsheet\IOFactory;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use Symfony\Component\Serializer\Encoder\CsvEncoder;

class BinaryReaderContext extends RawMinkContext
{
    private $createdFiles;

    /**
     * @Then I create a :extension file from binary response with name :name
     */
    public function iCreateFileFromBinaryResponseAndItShouldContains(string $extension, string $name)
    {
        $content = $this->getSession()->getPage()->getContent();

        $filename = __DIR__."/../selenium_upload/$name.".strtolower($extension);

        $this->createdFiles[] = $filename;

        $fp = fopen($filename, 'wb');
        fwrite($fp, $content);
        fclose($fp);

        if (!file_exists($filename)) {
            throw new \RuntimeException("'$filename' doesn't exists or was not created.");
        }
    }

    /**
     * @Given I have created :filename file :nbHours hours ago
     */
    public function iHaveCreatedFileAtHoursAgo(string $filename, int $nbHoursAgo): void
    {
        if (!file_exists($dir = pathinfo($filename)['dirname'])) {
            if (!mkdir($dir, 0775, true) && !is_dir($dir)) {
                throw new \RuntimeException(sprintf('Directory "%s" was not created', $dir));
            }
        }

        @touch($filename, (new DateTime(sprintf('-%d hours', $nbHoursAgo)))->getTimestamp());
        $this->createdFiles[] = $filename;
    }

    /**
     * @Then /^the file "(?P<filename>[^"]+)" (?P<exist>should|should not) exist$/
     */
    public function theFileShouldExist(string $filename, string $exist): void
    {
        if (('should' === $exist) !== file_exists($filename)) {
            throw new \RuntimeException(sprintf('%s %s exist', $filename, $exist));
        }
    }

    /**
     * @Then the :extension file :name should contain:
     * | cell | value |
     */
    public function theXlsCreatedFileShouldContains(string $extension, string $name, TableNode $nodes)
    {
        $spreadsheet = $this->readSpreadSheetFile($name, $extension);

        foreach ($nodes->getRowsHash() as $column => $expected) {
            $actual = $spreadsheet->getActiveSheet()->getCell($column)->getValue();
            if ($expected != $actual) {
                throw new \Exception(sprintf("The value is '%s', '%s' was expected ! ", $actual, $expected));
            }
        }
    }

    /**
     * @Then the :extension file :name should contain :value in column :column
     */
    public function theFileShouldContainValueInColumn(string $extension, string $name, string $value, string $column)
    {
        $spreadsheet = $this->readSpreadSheetFile($name, $extension);
        $highestRow = $spreadsheet->getActiveSheet()->getHighestRow();

        for ($row = 1; $row <= $highestRow; ++$row) {
            $cellValue = $spreadsheet->getActiveSheet()->getCell($column.$row)->getValue();

            if ($value === $cellValue) {
                return;
            }
        }

        throw new \Exception(sprintf("The value '%s' was not found in column '%s' !", $value, $column));
    }

    /**
     * @Then the :extension file :name should not contain :value in column :column
     */
    public function theFileShouldNotContainValueInColumn(string $extension, string $name, string $value, string $column)
    {
        $spreadsheet = $this->readSpreadSheetFile($name, $extension);

        $highestRow = $spreadsheet->getActiveSheet()->getHighestRow();

        for ($row = 1; $row <= $highestRow; ++$row) {
            $cellValue = $spreadsheet->getActiveSheet()->getCell($column.$row)->getValue();

            if ($value === $cellValue) {
                throw new \Exception(sprintf("The value '%s' in column '%s' was not expected ! ", $value, $column));
            }
        }
    }

    /**
     * @Then the Excel file with glob :glob should contain:
     * | cell | value |
     */
    public function theExcelFileWithGlobShouldContain(string $glob, TableNode $nodes)
    {
        $files = glob($glob);
        if (empty($files)) {
            throw new \LogicException(sprintf('No files found for "%s".', $glob));
        }
        $filepath = $files[0];

        $ext = pathinfo($filepath, PATHINFO_EXTENSION);
        $reader = IOFactory::createReader(ucfirst($ext));
        $spreadsheet = $reader->load($filepath);

        foreach ($nodes->getRowsHash() as $column => $expected) {
            $actual = $spreadsheet->getActiveSheet()->getCell($column)->getValue();
            if ($expected != $actual) {
                throw new \Exception(sprintf("The value is '%s', '%s' was expected ! ", $actual, $expected));
            }
        }
    }

    /**
     * @Then the XLS file at path :path should match table:
     */
    public function theXlsFileShouldMatchTable(TableNode $table, string $path): void
    {
        $filepath = $this->getFilePathAt($path);

        $ext = pathinfo($filepath, PATHINFO_EXTENSION);
        $reader = IOFactory::createReader(ucfirst($ext));
        $spreadsheet = $reader->load($filepath);

        foreach ($table->getRows() as $lineNumber => $line) {
            $col = 'A';
            $actualLine = $lineNumber + 1;
            foreach ($line as $key => $expectedValue) {
                $cell = $col.$actualLine;
                $actual = (string) $spreadsheet->getActiveSheet()->getCell($cell)->getValue();
                if ($expectedValue !== $actual) {
                    throw new \Exception(sprintf("The value is '%s', '%s' was expected ! ", $actual, $expectedValue));
                }
                ++$col;
            }
        }
    }

    /**
     * @AfterScenario @files
     */
    public function after()
    {
        if (!empty($this->createdFiles)) {
            foreach ($this->createdFiles as $filename) {
                unlink($filename);
            }
        }
    }

    /**
     * To generate a datetime use this placeholder : <datetime|(construct string)|(format string)> (example: <datetime|-1 day|Y-m-d H:i:s>).
     *
     * @Given I copy :source to :dest
     */
    public function iCopyTo(string $source, string $dest)
    {
        $dest = preg_replace_callback('~(?:<datetime\|(?P<datetime>[^|]*)\|(?P<format>[^|]*)>)~iU', static function ($match) {
            return (new \DateTimeImmutable($match['datetime']))->format($match['format']);
        }, $dest);

        if (!@mkdir($concurrentDirectory = pathinfo($dest)['dirname'], 0777, true) && !is_dir($concurrentDirectory)) {
            throw new \RuntimeException(sprintf('Directory "%s" was not created', $concurrentDirectory));
        }

        copy($source, $dest);
    }

    /**
     * @Given I copy with glob :source to :dest
     */
    public function iCopyWithGlob(string $source, string $dest)
    {
        shell_exec("cp $source $dest");
    }

    /**
     * To generate a datetime use this placeholder in a file : %datetime|(construct string)|(format string)% (example: %datetime|-1 day|Y-m-d H:i:s%).
     *
     * @Given I compute datetime placeholders in :filepath
     */
    public function iComputeDatetimePlaceholders(string $filepath)
    {
        $content = file_get_contents($filepath);

        $content = preg_replace_callback('~(?:%datetime\|(?P<datetime>[^|]*)\|(?P<format>[^|]*)%)~iU', static function ($match) {
            return (new \DateTimeImmutable($match['datetime']))->format($match['format']);
        }, $content);

        file_put_contents($filepath, $content);
    }

    /**
     * @Given I remove :glob
     */
    public function iRemove(string $glob)
    {
        $files = glob($glob);

        foreach ($files as $path) {
            if (is_file($path)) {
                unlink($path);

                continue;
            }
            if (!is_dir($path)) {
                continue;
            }
            $files = new RecursiveIteratorIterator(
                new RecursiveDirectoryIterator($path, RecursiveDirectoryIterator::SKIP_DOTS),
                RecursiveIteratorIterator::CHILD_FIRST
            );

            foreach ($files as $fileinfo) {
                $todo = ($fileinfo->isDir() ? 'rmdir' : 'unlink');
                $todo($fileinfo->getRealPath());
            }

            rmdir($path);
        }
    }

    /**
     * @Then the file :glob should contain:
     */
    public function theFileShouldContain(string $glob, PyStringNode $content)
    {
        $files = glob($glob);
        if (empty($files)) {
            throw new \LogicException(sprintf('No files found for "%s".', $glob));
        }
        $filepath = $files[0];

        $contents = file_get_contents($filepath);
        foreach ($content->getStrings() as $line) {
            if (false === strpos($contents, trim($line))) {
                throw new \LogicException(sprintf('The file "%s" does not contain: "%s".', $filepath, $content));
            }
        }
    }

    /**
     * @Given I set mode :mode to :glob
     */
    public function iSetModeTo(int $mode, string $glob)
    {
        $files = glob($glob);

        foreach ($files as $path) {
            if (!chmod($path, octdec($mode))) {
                throw new \RuntimeException(sprintf('Unable to chmod the file "%s"', $path));
            }
        }
    }

    /**
     * @Given I create folder :path
     */
    public function iCreateFolder($path)
    {
        if (file_exists($path)) {
            return;
        }

        if (!mkdir($path, 0755, true) && !is_dir($path)) {
            throw new \RuntimeException(sprintf('Directory "%s" was not created', $path));
        }
    }

    /**
     * @Then the :path1 file should be smaller than :path2
     */
    public function theFileShouldBeSmallerThanTheOther(string $path1, string $path2)
    {
        if (!(filesize($path1) < filesize($path2))) {
            throw new \RuntimeException(sprintf('The file "%s" is not smaller than "%s"', $path1, $path2));
        }
    }

    /**
     * @Then the CSV file at path :path with :separator separator should match table:
     * @Then the CSV file at path :path with :separator with encoding :encoding should match table:
     */
    public function theCSVFileShouldMatchTable(TableNode $table, string $path, string $separator, string $encoding = 'UTF-8'): void
    {
        $filepath = $this->getFilePathAt($path);

        $csv = [];
        $header = [];

        $file = fopen($filepath, 'rb+');
        while (($line = fgets($file)) !== false) {
            if (empty($header)) {
                $header = str_getcsv($line, $separator);
                if ('UTF-8' !== $encoding) {
                    $header = array_map(static function ($value) use ($encoding) {
                        return mb_convert_encoding($value, 'UTF-8', $encoding);
                    }, $header);
                }
            } else {
                $csv[] = str_getcsv($line, $separator);
            }
        }
        fclose($file);

        if ('UTF-8' !== $encoding) {
            $csv = self::changeEncoding($csv, $encoding);
        }

        foreach ($table->getHash() as $lineNumber => $line) {
            if (0 === $lineNumber && array_keys($line) !== $header) {
                throw new \Exception(sprintf('Expected row "%s", but row "%s" was found at line %d.', implode($separator, $line), isset($csv[$lineNumber]) ? implode($separator, $csv[$lineNumber]) : '', $lineNumber + 1));
            }

            if (0 !== $lineNumber) {
                foreach (array_values($line) as $columnNumber => $expectedValue) {
                    $matcherError = '';
                    if (!PHPMatcher::match($csv[$lineNumber][$columnNumber], $expectedValue, $matcherError)) {
                        throw new \Exception(sprintf('Expected row "%s", but row "%s" was found at line %d. %s.', implode($separator, $line), isset($csv[$lineNumber]) ? implode($separator, $csv[$lineNumber]) : '', $lineNumber + 1, $matcherError));
                    }
                }
            }
        }
    }

    /**
     * @Then the file :path in the temp directory should be the same as :fixture
     */
    public function theFileInTheTempDirectoryShouldBeTheSameAs(string $path, string $fixturePath): void
    {
        $filePath = $this->getFilePathAt(sys_get_temp_dir().'/'.$path);
        $fixtureFilePath = $this->getFilePathAt($fixturePath);

        if (base64_encode(file_get_contents($filePath)) !== base64_encode(file_get_contents($fixtureFilePath))) {
            throw new \Exception(sprintf('The file %s is not the same as %s', $filePath, $fixtureFilePath));
        }
    }

    /**
     * @Then the downloaded csv file :name should be the same as :fixture
     */
    public function theDownloadedCsvFileShouldBeTheSameAs(string $name, string $fixturePath): void
    {
        $filename = $this->getFilePathAt(__DIR__.'/../selenium_upload/'.$name);
        $fixtureFilePath = $this->getFilePathAt($fixturePath);
        $csvDecoder = new CsvEncoder();

        $actualCsvContent = $csvDecoder->decode(file_get_contents($filename), 'csv', [CsvEncoder::NO_HEADERS_KEY => true]);
        $expectedCsvContent = $csvDecoder->decode(file_get_contents($fixtureFilePath), 'csv', [CsvEncoder::NO_HEADERS_KEY => true]);

        if (\count($actualCsvContent) !== \count($expectedCsvContent)) {
            throw new \Exception(sprintf('The file "%s" does not have the same number of lines as %s', $filename, $fixtureFilePath));
        }

        for ($index = 0, $count = \count($actualCsvContent); $index < $count; ++$index) {
            if ([] !== array_diff_assoc($actualCsvContent[$index], $expectedCsvContent[$index])) {
                throw new \Exception(sprintf('At line #%d, expected row "%s" but found "%s".', $index + 1, implode(',', $expectedCsvContent[$index]), implode(',', $actualCsvContent[$index])));
            }
        }
    }

    private function getFilePathAt(string $path): string
    {
        $files = glob($path);

        if (empty($files)) {
            throw new \LogicException(sprintf('No files found for "%s".', $path));
        }

        return $files[0];
    }

    private static function changeEncoding(array $data, string $encoding): array
    {
        return array_map(static function (array $row) use ($encoding): array {
            return array_map(static function (string $column) use ($encoding): string {
                return mb_convert_encoding($column, 'UTF-8', $encoding);
            }, $row);
        }, $data);
    }

    private function readSpreadSheetFile(string $name, string $extension): Spreadsheet
    {
        $filename = __DIR__."/../selenium_upload/$name.".strtolower($extension);

        if (!file_exists($filename)) {
            throw new \RuntimeException("'$filename' doesn't exist.");
        }

        $reader = IOFactory::createReader(ucfirst($extension));

        return $reader->load($filename);
    }
}
