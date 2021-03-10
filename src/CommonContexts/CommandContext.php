<?php

namespace Proweb\CommonContexts;

use Behat\Gherkin\Node\PyStringNode;
use Behat\MinkExtension\Context\RawMinkContext;
use Behat\Symfony2Extension\Context\KernelAwareContext;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\Process\Process;

class CommandContext extends RawMinkContext implements KernelAwareContext
{
    /** @var KernelInterface */
    protected $kernel;

    protected $output;

    /** @var Process */
    protected $process;

    protected $confirm = false;

    /**
     * Add an automated confirm process.
     * It will respond "yes" in each command.
     *
     * @Given /^I will answer yes to the next commands$/
     */
    public function iWillAnswerYesToTheNextCommands(): void
    {
        $this->confirm = true;
    }

    /**
     * @Given /^I run "([^"]*)" command$/
     * @Given /^I run '(.*)' command$/
     */
    public function iRun($command): void
    {
        $env = $this->kernel->getEnvironment();
        $php = $this->getPHP();
        $this->output = $this->exec(sprintf('%s bin/console --env=%s %s', $php, $env, $command));
    }

    /**
     * @Given I run the following command:
     */
    public function iRunTheFollowing(PyStringNode $command): void
    {
        $strCommand = null !== $command ? implode(' ', $command->getStrings()) : null;
        $this->iRun($strCommand);
    }

    /**
     * @Given /^I run in bash '(.*)'/
     */
    public function iRunInBash($command): void
    {
        $this->output = $this->exec($command);
    }

    /**
     * @Given /^I run "([^"]*)" command in new process$/
     */
    public function iRunInNewProcess($command): void
    {
        $env = $this->kernel->getEnvironment();

        $commandline = sprintf('php bin/console --env=%s %s', $env, $command);
        $this->process = new Process($commandline);
        $this->process->start();
    }

    /**
     * @Then /^I should see "(?<string>.*)" in the console$/
     */
    public function iShouldSeeInTheConsole($string): void
    {
        if ($this->process) {
            $ttl = 0;
            while (false === strpos($this->getOutput(), $string)) {
                usleep(100000); // 0.1 second
                ++$ttl;

                if ($ttl > 30) { // 3 seconds
                    throw new \Exception(sprintf('Did not see "%s" in console "%s"', $string, $this->output));
                }
            }

            return;
        }

        if (false === strpos($this->getOutput(), $string)) {
            throw new \Exception(sprintf('Did not see "%s" in console "%s"', $string, $this->output));
        }
    }

    /**
     * @Then /^I should not see "(?<string>.*)" in the console$/
     */
    public function iShouldNotSeeInTheConsole($string): void
    {
        if (strpos($this->getOutput(), $string)) {
            throw new \Exception(sprintf('Did see "%s" in console "%s"', $string, $this->output));
        }
    }

    /**
     * @Then I stop the running process
     */
    public function iStopTheRunningProcess(): void
    {
        if ($this->process) {
            $this->process->stop();
        }
    }

    /**
     * @Then dump the command output
     */
    public function dumpTheCommandOutput(): void
    {
        echo $this->getOutput();
    }

    public function setKernel(KernelInterface $kernel)
    {
        $this->kernel = $kernel;
    }

    /**
     * Return the current console output.
     */
    protected function getOutput(): string
    {
        if ($this->process) {
            return $this->process->getOutput();
        }

        return $this->output;
    }

    /**
     * Return the php bin with the right configuration.
     */
    protected function getPHP(): string
    {
        $php = 'php';
        if (extension_loaded('xdebug')) {
            $php = sprintf(
                'PHP_IDE_CONFIG="%s" php -dzend_extension=xdebug.so -dxdebug.remote_enable=%s -dxdebug.remote_mode=%s -dxdebug.remote_port=%s -dxdebug.remote_host=%s',
                $_ENV['PHP_IDE_CONFIG'] ?? 'serverName=localhost',
                ini_get('xdebug.remote_enable'),
                ini_get('xdebug.remote_mode'),
                ini_get('xdebug.remote_port'),
                ini_get('xdebug.remote_host')
            );
        }

        return $php;
    }

    /**
     * Execute input with shell_exec.
     * Handle confirmation by responding "yes" in each command if enable.
     */
    protected function exec(string $input)
    {
        if ($this->confirm) {
            $input = sprintf('yes | %s', $input);
        }

        return shell_exec($input);
    }
}
