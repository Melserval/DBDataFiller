<?php
namespace DBDataFilling;

use \PDO;
use \PDOException;
use \Exception;

class FileDataInfo
{
    private $table_name;
    private $file_name;
    private $start_row;
    private $field_names;
    private $cells_numbers;
    private Array $db_config;
    private const BUFFER_SIZE = 750;

    public function __construct(string $tablename,
                                string $filename,
                                int $startrow,
                                array $columns,
                                array $db_config)
    {
        $this->table_name = $tablename;
        $this->file_name = $filename;
        $this->start_row = $startrow;
        $this->field_names = array_keys($columns);
        $this->cells_numbers = array_values($columns);
        $this->db_config = $db_config;
    }

    private function getQueryTemplate(int $str_set_num=1): string
    {
        $values_template = implode(', ', array_fill(0, count($this->cells_numbers), '?'));
        $query_template = sprintf(
            "INSERT INTO %s (%s) VALUES %s",
            $this->table_name,
            implode(', ', $this->field_names),
            implode(', ', array_fill(0, $str_set_num, "($values_template)"))
        );
        return $query_template;
    }

    private function process()
    {
        try {
            $pdo = new PDO(...$this->db_config);
            $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_NUM);

            if (!in_array([$this->table_name], $pdo->query("SHOW TABLES")->fetchall())) {
                throw new Exception("Неверное имя таблицы '{$this->table_name}'", 1);
            }

            $db_fields = $pdo->query("SHOW COLUMNS FROM {$this->table_name}")
                            ->fetchall(PDO::FETCH_COLUMN, 0);

            foreach ($this->field_names as $field) {
                if (!in_array($field, $db_fields)) {
                    throw new Exception("Ошибка! Неверное имя поля '{$field}'!", 1);
                }
            }

            if (false === ($fp = @fopen($this->file_name, 'r'))) {
                throw new Exception("Неудалось прочитать файл данных {$this->file_name}", 1);
            }

            $data_buffer = [];
            $buffer_state = 0;
            $stmh = $pdo->prepare($this->getQueryTemplate(self::BUFFER_SIZE));

            for ($i = 0; $row = fgets($fp); $i++) {
                if ($i < $this->start_row) {
                    continue;
                }
                $data = explode(',', trim($row));
                foreach ($this->cells_numbers as $num => $column) {
                    $data_buffer[] = $data[$column];
                }
                $buffer_state++;
                if ($buffer_state == self::BUFFER_SIZE) {
                    $stmh->execute($data_buffer);
                    $data_buffer = [];
                    $buffer_state = 0;
                }
            }
            if ($buffer_state > 0) {
                $pdo->prepare($this->getQueryTemplate($buffer_state))->execute($data_buffer);
            }
            // TODO: Добавить проверку был ли файл считан до конца.
            fclose($fp);
        } catch (PDOException $e) {
            print $e->getmessage();
        } catch (Exception $e) {
            print $e->getmessage();
        }
    }

    public function run()
    {
        $this->process();
    }
}