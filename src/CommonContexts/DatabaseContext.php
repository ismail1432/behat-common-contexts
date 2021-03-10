<?php

namespace Proweb\CommonContexts;

use Behat\Behat\Context\Context;
use Symfony\Component\DependencyInjection\ContainerInterface;

class DatabaseContext implements Context
{
    private $container;

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    /**
     * Check to have a given element in the database.
     *
     * @Then In the :db db I should have element :id in :table with the following values:
     * @Then In the :db db I should have element :id in column :columnName in :table with the following values:
     * @Then In the :db db I should have element :id in :table with newline :newline with the following values:
     * @Then In the :db db I should have element :id in column :columnName in :table with newline :newline with the following values:
     */
    public function InTheDBIShouldHaveTable(TableNode $nodes, string $db, int $id, string $table, string $columnName = 'id', ?string $newline = null): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT * FROM '.$table.' WHERE '.$columnName.' = :value';
        $result = $connexion->executeQuery($query, ['value' => $id])->fetchAll();

        if (empty($result)) {
            throw new \LogicException(sprintf('Could not find row in %s.%s (where %s=%d)', $db, $table, $columnName, $id));
        }

        // Retrieve the first and only result
        $result = $result[0];
        $nodesTable = $nodes->getTable();
        $violations = [];
        foreach ($nodesTable as $row) {
            if (!array_key_exists($row[0], $result)) {
                throw new \LogicException(sprintf('In %s db, %s.%s column does not exist.', $db, $table, $row[0]));
            }

            $value = $newline ? str_replace($newline, PHP_EOL, $row[1]) : $row[1];
            if ($result[$row[0]] != $value) {
//                throw new \LogicException(sprintf('In %s db, %s.%s should be equal to "%s". Current value is "%s".', $db, $table, $row[0], $row[1], $result[$row[0]]));
                $violations[] = sprintf('In %s db, %s.%s should be equal to "%s". Current value is "%s".', $db, $table, $row[0], $row[1], $result[$row[0]]);
            }
        }

        if (!empty($violations)) {
            throw new \LogicException(implode(PHP_EOL, $violations));
        }
    }

    /**
     * Check encrypted values.
     *
     * @Then In the :db db I should have element :id in :table with the following encrypted values:
     */
    public function InTheDBIShouldHaveTableEncrypted(TableNode $nodes, string $db, int $id, string $table, string $columnName = 'id'): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT * FROM '.$table.' WHERE '.$columnName.' = :value';
        $result = $connexion->executeQuery($query, ['value' => $id])->fetchAll();

        if (empty($result)) {
            throw new \LogicException(sprintf('Could not find row in %s.%s (where %s=%d)', $db, $table, $columnName, $id));
        }

        /** @var \Ambta\DoctrineEncryptBundle\Encryptors\EncryptorInterface $encryptor */
        $encryptor = $this->container->get('ambta_doctrine_encrypt.encryptor');

        // Retrieve the first and only result
        $result = $result[0];
        $nodesTable = $nodes->getTable();
        $violations = [];
        foreach ($nodesTable as $row) {
            if (!array_key_exists($row[0], $result)) {
                throw new \LogicException(sprintf('In %s db, %s.%s column does not exist.', $db, $table, $row[0]));
            }

            $value = $row[1];
            $persistedValue = $result[$row[0]];

            if (DoctrineEncryptSubscriber::ENCRYPTION_MARKER === substr($persistedValue, -strlen(DoctrineEncryptSubscriber::ENCRYPTION_MARKER))) {
                $persistedValue = $encryptor->decrypt(substr($persistedValue, 0, -5));
                if ($persistedValue !== $value) {
                    $violations[] = sprintf('In %s db, %s.%s should be equal to "%s". Current value is "%s".', $db, $table, $row[0], $value, $persistedValue);
                }
            } else {
                $violations[] = sprintf('In %s db, %s.%s cannot be decrypted. Current value is "%s". Are you sure this is an encrypted column?', $db, $table, $row[0], $result[$row[0]]);
            }
        }

        if (!empty($violations)) {
            throw new \LogicException(implode(PHP_EOL, $violations));
        }
    }

    /**
     * Count how many element of a table have some values in the database.
     *
     * @Then In the :db db I should not have elements in :table with the following values:
     * @Then In the :db db I should have :count elements in :table with the following values:
     */
    public function InTheDBIShouldCountElementsWithValues(TableNode $nodes, string $db, string $table, int $count = 0): void
    {
        $nodesTable = $nodes->getTable();
        if (0 === count($nodesTable)) {
            throw new \LogicException('The Table must have at least 1 row.');
        }

        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT COUNT(*) as countRows FROM '.$table.' WHERE ';

        $params = [];
        $i = 0;
        // build WHERE conditions with AND
        $query .= implode(' AND ', array_map(static function ($row) use (&$i, &$params) {
            [$key, $value] = $row;
            ++$i;
            $params[':value_'.$i] = $value;

            return " $key = :value_$i ";
        }, $nodesTable));

        $result = $connexion->executeQuery($query, $params)->fetch();

        if ($count !== (int) $result['countRows']) {
            throw new \LogicException(sprintf('%s rows have been found but %d expected.', $result['countRows'], $count));
        }
    }

    /**
     * @Then In the :db db I should have element :id in :table with :column looks like :value
     * @Then In the :db db I should have element :id in column :columnName in :table with :column looks like :value
     */
    public function InTheDBIShouldHaveTheField(string $column, string $value, string $db, int $id, string $table, string $columnName = 'id'): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = sprintf('SELECT * FROM %s WHERE %s = :id AND %s LIKE :value', $table, $columnName, $column);
        $result = $connexion->executeQuery($query, ['id' => $id, 'value' => $value])->fetchAll();

        if (empty($result)) {
            throw new \LogicException(sprintf('Item #%s from table "%s" has no column %s that looks like %s.!', $id, $table, $column, $value));
        }
    }

    /**
     * Check to not have a given element.
     *
     * @Then In the :db db I should not have element :id in :table
     * @Then In the :db db I should not have element :id in column :columnName in :table
     */
    public function InTheDBIShouldNotHave(string $db, int $id, string $table, string $columnName = 'id'): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT * FROM '.$table.' WHERE '.$columnName.' = :id';
        $result = $connexion->executeQuery($query, ['id' => $id])->fetchAll();

        if (!empty($result)) {
            throw new \LogicException(sprintf('Item #%s from table "%s" WAS found!', $id, $table));
        }
    }

    /**
     * Check to not have a given element in a cell.
     *
     * @Then In the :db db, value in :column in :table in element :identifier should not be equal to :value
     */
    public function InTheDBColumnIShouldNotHave(string $db, string $column, string $table, string $identifier, string $value): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = "SELECT $column FROM $table WHERE id=$identifier";
        $result = $connexion->executeQuery($query)->fetchAll();

        if (empty($result)) {
            throw new \LogicException(sprintf('Empty result for query %s', $query));
        } elseif (count($result) > 1) {
            throw new \LogicException(sprintf('Mutliple rows found for query %s!', $query));
        }

        if ($result[0][$column] === $value) {
            throw new \LogicException(sprintf('Both values are identical %s', $value));
        }
    }

    /**
     * Check to have a given element.
     *
     * @Then In the :db db I should have element :id in :table
     * @Then In the :db db I should have element :id in column :columnName in :table
     */
    public function InTheDBIShouldHave(string $db, int $id, string $table, string $columnName = 'id'): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT 1 FROM '.$table.' WHERE '.$columnName.' = :id';
        $result = $connexion->executeQuery($query, ['id' => $id])->fetchAll();

        if (empty($result)) {
            throw new \LogicException(sprintf('Item #%s from table "%s" was NOT found!', $id, $table));
        }
    }

    /**
     * @Then In the :db db I should have compound element :element identified by :columns in :table
     */
    public function inTheDbIShouldHaveCompoundElementIn($db, $element, $columns, $table): void
    {
        $columns = array_map('trim', explode(',', $columns));
        $values = array_map('trim', explode(',', $element));

        if (\count($columns) !== \count($values)) {
            throw new \LogicException('The number of columns does not match the number of values.');
        }

        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $conditions = [];
        foreach (array_combine($columns, $values) as $column => $value) {
            $conditions[] = $column.' = "'.$value.'"';
        }

        $query = 'SELECT count(*) as count FROM '.$table.' WHERE '.implode(' AND ', $conditions);

        $result = $connexion->executeQuery($query)->fetch();

        if (0 === (int) $result['count']) {
            throw new \LogicException(sprintf('Compound element %s identified by %s was not found.', $element, implode(', ', $columns)));
        }
    }

    /**
     * @Then In the :db db I should have compound element :element identified by :columns in :table with the following values:
     */
    public function inTheDbIShouldHaveCompoundElementInWithTheFollowingValues(TableNode $nodes, $element, $columns, $table, $db = 'default'): void
    {
        $columns = array_map('trim', explode(',', $columns));
        $values = array_map('trim', explode(',', $element));

        if (\count($columns) !== \count($values)) {
            throw new \LogicException('The number of columns does not match the number of values.');
        }

        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $conditions = [];
        foreach (array_combine($columns, $values) as $column => $value) {
            $conditions[] = $column.' = "'.$value.'"';
        }

        $nodesTable = $nodes->getTable();

        $query = 'SELECT '.implode(',', array_column($nodesTable, 0)).' FROM '.$table.' WHERE '.implode(' AND ', $conditions);

        $result = $connexion->executeQuery($query)->fetch();

        foreach ($nodesTable as $row) {
            if (!array_key_exists($row[0], $result)) {
                throw new \LogicException(sprintf('In %s db, %s.%s column does not exist.', $db, $table, $row[0]));
            }

            if ($result[$row[0]] != $row[1]) {
                throw new \LogicException(sprintf('In %s db, %s.%s should be equal to "%s". Current value is "%s".', $db, $table, $row[0], $row[1], $result[$row[0]]));
            }
        }
    }

    /**
     * @Then In the :db db I should not have compound element :element identified by :columns in :table
     */
    public function inTheDbIShouldNotHaveCompoundElementIn($db, $element, $columns, $table): void
    {
        $columns = array_map('trim', explode(',', $columns));
        $values = array_map('trim', explode(',', $element));

        if (\count($columns) !== \count($values)) {
            throw new \LogicException('The number of columns does not match the number of values.');
        }

        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $conditions = [];
        foreach (array_combine($columns, $values) as $column => $value) {
            $conditions[] = $column.' = "'.$value.'"';
        }

        $query = 'SELECT count(*) as count FROM '.$table.' WHERE '.implode(' AND ', $conditions);

        $result = $connexion->executeQuery($query)->fetch();

        if (0 !== (int) $result['count']) {
            throw new \LogicException(sprintf('Compound element %s identified by %s was found but it should not be.', $element, implode(', ', $columns)));
        }
    }

    /**
     * @Then In the :db db I should have element :id in :table with date column :dateColumn equal to :date with format :format
     * @Then In the :db db I should have element :id in column :columnName in :table with date column :dateColumn equal to :date with format :format
     */
    public function InTheDBIShouldHaveDynamicDate(string $db, int $id, string $table, string $dateColumn, string $date, string $format, string $columnName = 'id'): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT * FROM '.$table.' WHERE '.$columnName.' = :value';
        $result = $connexion->executeQuery($query, ['value' => $id])->fetchAll();

        if (empty($result)) {
            throw new \LogicException(sprintf('Item #%s from table "%s" was not found', $id, $table));
        }

        // Retrieve the first and only result
        $result = $result[0];

        $formattedDate = (new \DateTimeImmutable($date))->format($format);
        if (!array_key_exists($dateColumn, $result)) {
            throw new \LogicException(sprintf('Column "%s" from table "%s" was not found', $dateColumn, $table));
        }
        $formattedFetchedDate = !empty($result[$dateColumn]) ? (new \DateTimeImmutable($result[$dateColumn]))->format($format) : null;
        if ($formattedFetchedDate !== $formattedDate) {
            throw new \LogicException(sprintf('Value "%s" for column "%s" from table "%s" was not found', $formattedDate, $dateColumn, $table));
        }
    }

    /**
     * @Then In the :db db the column :columnName in :table with identifier :identifier and id :id should be null
     * @Then In the :db db The column :columnName in :table with id :id should be null
     */
    public function InTheDbTheColumnShouldBeNull(string $db, string $table, int $id, string $columnName, string $identifier = 'id'): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT '.$columnName.' FROM '.$table.' WHERE '.$identifier.' ='.$id;
        $result = $connexion->executeQuery($query, ['value' => $id])->fetch()[$columnName];

        if (null !== $result) {
            throw new \LogicException(sprintf('The Column "%s" in table "%s" should be null but its value is "%s"', $columnName, $table, $result));
        }
    }

    /**
     * @Then In the :db db The column :columnName in :table with id :id should not be null
     * @Then In the :db db The column :columnName in :table with identifier :identifier equal :id should not be null
     */
    public function InTheDbTheColumnShouldNotBeNull(string $db, string $table, int $id, string $columnName, string $identifier = 'id'): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT '.$columnName.' FROM '.$table.' WHERE '.$identifier.' ='.$id;
        $result = $connexion->executeQuery($query, ['value' => $id])->fetch()[$columnName];

        if (null === $result) {
            throw new \LogicException(sprintf('The Column "%s" in table "%s" with id %s is null, not null expected', $columnName, $table, $id));
        }
    }

    /**
     * @Then The :db db with the column :columnName in :table should have :total element(s)
     * @Then The :db db with the column :columnName with value :columnValue in :table should have :total element(s)
     */
    public function theDbWithColumnInTableShouldHaveTotalElements(string $db, string $table, string $columnName, int $totalExpected, ?string $columnValue = null): void
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        $query = 'SELECT COUNT('.$columnName.') AS TOTAL FROM '.$table;

        if (!empty($columnValue)) {
            $query .= ' WHERE '.$columnName.' = "'.$columnValue.'"';
        }

        $total = $connexion->executeQuery($query)->fetch()['TOTAL'];

        if ($total != $totalExpected) {
            throw new \LogicException(sprintf('The table "%s" have %s elements, %s expected!', $table, $total, $totalExpected));
        }
    }

    /**
     * @Then In the :db db I should have elements in :table1 joined with :table2 on :joinPart1 and :joinPart2 with the following values:
     * @Then In the :db db I should have elements in :table1 with the following values:
     */
    public function inTheDBLegacyIShouldHaveElementsInTableJoinedWithTableWithValuesOnJoin(
        TableNode $nodes,
        string $db,
        string $table1,
        ?string $table2 = null,
        ?string $joinPart1 = null,
        ?string $joinPart2 = null
    ) {
        $conn = $this->container->get(sprintf('doctrine.dbal.%s_connection', $db));

        $rows = $nodes->getTable();
        $headers = array_shift($rows);
        $violationsRows = [];
        foreach ($rows as $iRow => $row) {
            if (null !== $table2) {
                $query = "SELECT COUNT(*) as countRows FROM $table1 LEFT JOIN $table2 ON $joinPart1 = $joinPart2 WHERE 1=1 ";
            } else {
                $query = "SELECT COUNT(*) as countRows FROM $table1 WHERE 1=1 ";
            }

            $params = [];
            $i = 0;
            // build WHERE conditions with AND
            foreach ($row as $iHeader => $value) {
                $key = $headers[$iHeader];
                ++$i;
                $params[':value_'.$i] = $value;
                $query .= " AND $key = :value_$i ";
            }

            $result = $conn->executeQuery($query, $params)->fetch();

            if (0 === (int) $result['countRows']) {
                $violationsRows[] = $iRow + 1;
            }
        }

        if ([] !== $violationsRows) {
            throw new \LogicException(sprintf('No rows have been found with the values of following rows: %s.', implode(', ', $violationsRows)));
        }
    }

    /**
     * @Then the query :sql is executed in :db
     * @Then the following query is executed in :db:
     */
    public function theQueryIsExecutedInTable(string $db, ?string $sql = '', ?PyStringNode $body = null)
    {
        $connexion = $this->container->get('doctrine.dbal.'.$db.'_connection');

        try {
            $connexion->executeQuery($sql ?: (string) $body);
        } catch (Exception $exception) {
            throw new \LogicException((sprintf('The query %s cannot be executed : %s', $sql, $exception->getMessage())));
        }
    }

    private function checkElementUpdated(TableNode $nodes, array $result): bool
    {
        foreach ($nodes->getRowsHash() as $node => $text) {
            if (array_key_exists($node, $result) && $text !== $result[$node]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Compare a value fetched from database to another value fetched in another database.
     *
     * @Then The value in db :db in column :column and in :table with id :id is equal to the value in the :secondDb with column :secondColumn in :secondTable and identifier :identifier equal to :identifierValue
     */
    public function theValueInDatabaseShouldBeEqualToTheValueInOtherDatabase(
        string $db,
        string $column,
        string $table,
        int $id,
        string $secondDb,
        string $secondColumn,
        string $secondTable,
        string $identifierValue,
        string $identifier = 'id'
    ): void {
        // Fetch Value from 1st database
        $value = $this->fetch($db, $column, $table, 'id', $id);

        // Fetch value to comapre from 2nd database
        $valueToCompare = $this->fetch($secondDb, $secondColumn, $secondTable, $identifier, $identifierValue);

        if ($value != $valueToCompare) {
            throw new \LogicException(sprintf('The values are not equals ! The column "%s" in table : "%s" have the value "%s" and the column "%s" in table : "%s" have the value %s', $column, $table, $value, $secondColumn, $secondTable, $valueToCompare));
        }
    }

    private function fetch(string $database, string $column, string $table, string $identifier, string $identifierValue): ?string
    {
        $connexion = $this->container->get('doctrine.dbal.'.$database.'_connection');
        $query = "SELECT $column FROM $table WHERE $identifier = $identifierValue";

        return $connexion->executeQuery($query)->fetch()[$column];
    }
}
