<?php

namespace DataBreakers\Keboola;

use League\Csv\Reader;
use Symfony\Component\Yaml\Yaml;

use DataBreakers\Keboola\Config;
use DataBreakers\Keboola\UsersTable;
use DataBreakers\Keboola\ItemsTable;
use DataBreakers\Keboola\InteractionsTable;

use DataBreakers\DataApi;
use DataBreakers\Keboola\DataApi\ForceClient;

class Keboola {

	protected $arguments;
	protected $config;
	protected $client;

	protected $itemsFilePath;
	protected $usersFilePath;
	protected $interactionsFilePath;

	/**
	 *
	 */
	public function __construct() {
		$this->initArguments();
		$this->initConfig();
		$this->initInputMapping();
		$this->initClient();
	}

	/**
	 *
	 */
	public function initArguments() {
		// script arguments
		$arguments = getopt("d::", array('data::'));

		if( ! isset($arguments['data'])) {
			throw new Exception('Data folder not set.');
		}

		$this->arguments = $arguments;

		return $this;
	}

	/**
	 *
	 */
	public function initConfig() {
		$filePath = $this->arguments["data"] . '/config';
		if( is_readable($filePath.'.yml')) {
			$configArray = Yaml::parse(file_get_contents($filePath.'.yml'));
		} elseif( is_readable($filePath.'.json')) {
			$configArray = json_decode(file_get_contents($filePath.'.json'), true);
		} else {
		    throw new Exception('Missing config file');
		}

		try {
			$this->config = new Config($configArray);
		} catch(Exception $e) {
			print $e->getMessage();
			throw new Exception('Failed to init config file');
		}

		return $this;
	}

	/**
	 *
	 */
	public function initInputMapping() {
		$filePathPattern = $this->arguments["data"].'/in/tables/%s';

		foreach($this->config->getConfigArray()['storage']['input']['tables'] as $table) {
			$fileName = isset($table['destination']) ? $table['destination'] : $table['source'];

			if($fileName == 'items.csv') {
				$this->itemsFilePath = sprintf($filePathPattern, $fileName);
			} elseif($fileName == 'users.csv') {
				$this->usersFilePath = sprintf($filePathPattern, $fileName);
			} elseif($fileName == 'interactions.csv') {
				$this->interactionsFilePath = sprintf($filePathPattern, $fileName);
			} else {
				print "Skipping invalid table \"{$fileName}\".\n";
			}
		}
	}

	/**
	 *
	 */
	public function initClient() {
		try {
			$this->client = new ForceClient($this->config->getAcountId(), $this->config->getSecretKey());
		} catch(Exception $e) {
			print $e->getMessage();
			throw new Exception('Cannot init DataBreakers client');
		}
	}

	/**
	 *
	 */
	public function run() {
		// 1) create and validate tables
		$interactionsTable = new InteractionsTable($this->interactionsFilePath, $this->config);
		$usersTable = new UsersTable($this->usersFilePath, $this->config);
		$itemsTable = new ItemsTable($this->itemsFilePath, $this->config);

		// 2) clear old data
		try {
			$interactionsTable->clear($this->client);
			$usersTable->clear($this->client);
			$itemsTable->clear($this->client);
		} catch(Exception $e) {
			print $e->getMessage();
			throw new Exception("Table failed to clear old data\n");
		}

		// 3) set new structures
		try {
			$usersTable->setAttributes($this->client);
			$itemsTable->setAttributes($this->client);
		} catch(Exception $e) {
			print $e->getMessage();
			throw new Exception("Table failed to set defined attributes\n");
		}

		// 4) upload data
		try {
			$usersTable->upload($this->client);
			$itemsTable->upload($this->client);
			$interactionsTable->upload($this->client);
		} catch(Exception $e) {
			print $e->getMessage();
			throw new Exception("Table failed to insert data\n");
		}
	}

}
