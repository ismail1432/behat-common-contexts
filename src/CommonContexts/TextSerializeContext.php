<?php

namespace Proweb\CommonContexts;

use Behat\Gherkin\Node\TableNode;
use Behat\MinkExtension\Context\RawMinkContext;
use Behatch\HttpCall\Request;

class TextSerializeContext extends RawMinkContext
{
    protected $request;

    public function __construct(Request $request)
    {
        $this->request = $request;
    }

    /**
     * Sends a HTTP request with a some parameters.
     *
     * @Given I send a :method request to :url with form body:
     * @Given I send a :method request to :url with form body encode in :encode:
     */
    public function iSendARequestWithFormParameters(TableNode $data, $method, $url, $encode = 'utf-8')
    {
        $parameters = [];

        foreach ($data->getRowsHash() as $node => $text) {
            if ('utf-8' === $encode) {
                $parameters[$node] = $text;
            } else {
                $parameters[$node] = utf8_decode($text);
            }
        }

        $this->request->setHttpHeader('Content-Type', 'application/x-www-form-urlencoded');

        $this->request->send(
            $method,
            $this->locatePath($url),
            $parameters,
            []
        );
    }

    /**
     * Transform serialiazed content to an array to check the given values https://www.php.net/manual/fr/function.serialize.php.
     *
     * @Then the Text nodes should be equal to:
     */
    public function theTextNodesShouldBeEqualTo(TableNode $nodes)
    {
        $response = unserialize($this->getSession()->getPage()->getContent());

        foreach ($nodes->getRowsHash() as $node => $text) {
            if (!isset($response[$node])) {
                throw new \Exception(sprintf("The node value '%s' is not defined in response", $node));
            }

            if ($response[$node] != $text) {
                throw new \Exception(sprintf("The node value '%s' is not equal to '%s'", $node, $text));
            }
        }
    }
}
