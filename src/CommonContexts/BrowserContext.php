<?php

namespace Proweb\CommonContexts;

use Behat\Mink\Exception\ExpectationException;
use WebDriver\Exception\StaleElementReference;

class BrowserContext extends Behatch\Context\BrowserContext
{
    /**
     * @Then (I ) wait :seconds second(s) until the element :element is enabled
     */
    public function iWaitUntilTheElementIsEnabled(int $count, string $element): void
    {
        $startTime = time();

        do {
            try {
                $this->theElementShouldBeEnabled($element);

                return;
            } catch (ExpectationException $e) {
                /* Intentionally leave blank */
            } catch (StaleElementReference $e) {
                // assume page reloaded whilst we were still waiting
            }
        } while (time() - $startTime < $count);

        throw new exception(sprintf('Field "%s" is not enabled after %d seconds', $element, $count));
    }

    /**
     * @Then (I ) wait :seconds second(s) until the element :element is disabled
     */
    public function iWaitUntilTheElementIsDisabled(int $count, string $element): void
    {
        $startTime = time();

        do {
            try {
                $this->theElementShouldBeDisabled($element);

                return;
            } catch (ExpectationException $e) {
                /* Intentionally leave blank */
            } catch (StaleElementReference $e) {
                // assume page reloaded whilst we were still waiting
            }
        } while (time() - $startTime < $count);

        throw new exception(sprintf('Field "%s" is not disabled after %d seconds', $element, $count));
    }

    /**
     * Checks, that the page should contains specified text after given timeout.
     *
     * @Then (I )wait :count second(s) until I don't see :text
     */
    public function iWaitSecondsUntilIDontSee($count, $text)
    {
        $this->iWaitSecondsUntilIDontSeeInTheElement($count, $text, 'html');
    }

    /**
     * Checks, that the element don't contains specified text after timeout.
     *
     * @Then (I )wait :count second(s) until I don't see :text in the :element element
     */
    public function iWaitSecondsUntilIDontSeeInTheElement($count, $text, $element)
    {
        $startTime = time();
        $this->iWaitSecondsForElement($count, $element);

        $expected = str_replace('\\"', '"', $text);
        $message = "The text '$expected' was not found after a $count seconds timeout";

        $found = false;
        do {
            try {
                usleep(1000);
                $node = $this->getSession()->getPage()->find('css', $element);
                $this->assertNotContains($expected, $node->getText(), $message);

                return;
            } catch (ExpectationException $e) {
                /* Intentionally leave blank */
            } catch (StaleElementReference $e) {
                // assume page reloaded whilst we were still waiting
            }
        } while (!$found && (time() - $startTime < $count));

        // final assertion...
        $node = $this->getSession()->getPage()->find('css', $element);
        $this->assertNotContains($expected, $node->getText(), $message);
    }
}
