<?php
namespace DBDataFilling;


require __DIR__.'/FileDataInfo.php';

$config = parse_ini_file(__DIR__.'/config.ini');

$json_config = $config['json'];
$db_config = [$config['db_conf'], $config['db_user'], $config['db_pass']];

$set_FileDataInfo = [];
if (is_readable($json_config) && !is_dir($json_config)) {
	foreach (json_decode(file_get_contents($json_config), true) as $item) {
		foreach ($item['files'] as $item_file) {
			$set_FileDataInfo[] = new FileDataInfo($item['tablename'],
												   $item_file['filename'],
												   $item_file['startrow'],
												   $item_file['columns'],
												   $db_config);
		}
	}
	$start = microtime(true);
	array_walk($set_FileDataInfo, fn($item) => $item->run());
	$end = microtime(true);

	print $end - $start;
} else {
	print "Неудалось прочитать файл $json_config";
}
