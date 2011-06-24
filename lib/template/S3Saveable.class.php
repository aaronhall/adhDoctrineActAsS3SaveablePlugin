<?php

/**
 * @todo support custom S3 meta headers and request headers (see S3::putObject)
 * @author Aaron Hall <adhall@gmail.com>
 */
class S3Saveable extends Doctrine_Template
{

  protected $_options = array(
    's3_access_key_config_key'    => 's3_access_key',
    's3_secret_key_config_key'    => 's3_secret_key',
    's3_bucket_config_key'        => 's3_bucket',

    'ignore_original_extension'   => false,
    'do_guess_extension'          => true,
    'force_extension'             => false,

    'delete_local_on_save'        => false,

    's3_dir'                      => false,
    's3_base_filename'            => false,
    's3_acl'                      => S3::ACL_PUBLIC_READ,

    'generate_random_filename'    => true,
    'ensure_unique_local'         => true,
    'ensure_unique_remote'        => false,

    'columns' => array(
      'path'        => 's3_path',
      'is_saved'    => 's3_is_saved',
      'is_deleted'  => 's3_is_deleted',
    ),

    'file' => false,
  );

  protected static $s3_instance;
  protected $file, $listener;



  public function setTableDefinition() {
    $this->hasColumn($this->getColumn('path'), 'string', 255);
    $this->hasColumn($this->getColumn('is_saved'), 'boolean', null, array(
        'notnull' => true,
        'default' => false,
    ));
    $this->hasColumn($this->getColumn('is_deleted'), 'boolean', null, array(
        'notnull' => true,
        'default' => false,
    ));

    $this->addListener(new S3SaveableListener($this->_options));
  }

  /**
   * Instantiate S3 singleton
   *
   * @return S3 the instance
   */
  protected function initializeS3() {
    if(false === (self::$s3_instance instanceof S3)) {
      self::$s3_instance = new S3($this->getAppConfig('s3_access_key'), $this->getAppConfig('s3_secret_key'));
    }

    return self::$s3_instance;
  }

  /**
   * Get app.yml config for one of $_options['*_config_key']
   *
   * @param string $type Like 's3_access_key'
   * @throws S3SaveableException when key wasn't set or is falsey
   * @return string The param value
   */
  protected function getAppConfig($type) {
    $key = $this->_options[$type.'_config_key'];

    if (false === ($val = sfConfig::get('app_'.$key, false))) {
      throw new S3SaveableException("Could not find configuration key in app.yml ({$type}_config_key => {$key})");
    }
    
    return $val;
  }

  /**
   * Get the MySQL column name from configuration
   * 
   * @param string $key The $_options['columns'] key
   * @throws S3SaveableException when column wasn't set or was empty in $_options
   * @return string The column name
   */
  protected function getColumn($key) {
    $column = $this->_options['columns'][$key];
    if(empty($column)) {
      throw new S3SaveableException("No column name given for {$column}");
    }

    return $column;
  }



  /**
   * Initialize the S3 upload with a local file path to be saved when
   * Doctrine_Record::save() is called. This will only affect inserts, not updates.
   *
   * @param string $filename Absolute path to local file
   * @param array $options Overrides any default options
   * @throws S3Exception when validator throws sfValidatorError (this should never happen)
   * @return Doctrine_Record
   */
  public function fromLocalPath($filename, array $options=null) {
    if($options) {
      $this->_options = Doctrine_Lib::arrayDeepMerge($this->_options, $options);
    }

    if(false === file_exists($filename)) {
      throw new S3Exception('File does not exist');
    }

    if(false === is_readable($filename)) {
      throw new S3Exception('File is not readable');
    }

    $validator = new sfValidatorFile();

    try {
      $file = $validator->clean(array(
        'tmp_name'  => $filename,
        'name'      => basename($filename),
      ));
    } catch(sfValidatorError $e) {
      throw new S3Exception('File error: ' . $e->getMessage());
    }

    return $this->fromValidatedFile($file);
  }

  /**
   * Initialize the S3 upload with an sfValidatedFile (returned from the sfValidatorFile
   * object in doClean) to be saved when Doctrine_Record::save() is called. This
   * will only affect inserts, not updates.
   *
   * @param sfValidatedFile $file
   * @param array $options Overrides any default options
   * @return Doctrine_Record
   */
  public function fromValidatedFile(sfValidatedFile $file, array $options=null) {
    if($options) {
      Doctrine_Lib::arrayDeepMerge($this->_options, $options);
    }

    $this->file = $file;

    return $this->getInvoker();
  }



  /**
   * Even though this is public, this should not be called directly. Use
   * {@see fromLocalPath()} or {@see fromValidatedFile()}.
   *
   * @throws S3Exception on S3 PUT failure
   * @return Doctrine_Record
   */
  public function putObject() {
    if(false === $this->file instanceof sfValidatedFile) {
      return false;
    }

    $this->initializeS3();
    $invoker = $this->getInvoker();

    $path_original = $this->file->getTempName();
    $path_s3 = $this->buildS3Uri();

    $success = S3::putObject(
      S3::inputFile($path_original),
      $this->getAppConfig('s3_bucket'),
      $path_s3,
      $this->_options['s3_acl']
    );

    if(false === $success) {
      throw new S3Exception('Could not write S3 object');
    }

    $invoker[$this->getColumn('path')] = $path_s3;
    $invoker[$this->getColumn('is_saved')] = true;

    if($this->_options['delete_local_on_save']) {
      unlink($path_original);
    }

    return $invoker;
  }

  /**
   * Build the S3 URI from options. If `s3_base_filename` wasn't provided and
   * `generate_random_filename` is not false, tries generating a random, unique
   * filename (@see self::getRandomFilename).
   *
   * @throws S3Exception when custom s3_base_filename was given, wasn't unique, and randomly generated filename isn't allowed by configuration
   * @throws S3SaveableException when a unique filename couldn't be generated in 10 attempts
   * @return string The S3 URI
   */
  protected function buildS3Uri() {
    $dir = $this->_options['s3_dir'] ? $this->_options['s3_dir'] : '';
    $base_filename = $this->_options['s3_base_filename'];
    $extension = $this->determineExtension();

    $custom_path = self::getPathFromParts($dir, $base_filename, $extension);
    if($base_filename && $this->isUniquePath($custom_path)) {
      return $custom_path;
    } elseif($this->_options['generate_random_filename'] === false) {
      $e_msg = 'Custom filename was required and ' . ($base_filename ? 'is not unique' : 'was not provided');
      throw new S3Exception($e_msg);
    }

    // continue generating a random filename until it meets uniqueness criteria
    $tries = 0;
    do {
      $base_filename = self::getRandomFilename();
      $path = self::getPathFromParts($dir, $base_filename, $extension);

      if($tries === 10) {
        throw new S3SaveableException('Could not find random unique filename after 10 tries');
      }
      ++$tries;
    } while($this->isUniquePath($path) === false);

    return $path;
  }

  /**
   * Figure out the extension that's going to be used for the S3 URI. Checks for
   * the 'force_extension' option, which overrides everything. Then tries using
   * the original file's extension and, failing that, tries guessing from the file's
   * MIME type (@see sfValidatedFile).
   *
   * @return string The file extension, with a dot.
   */
  protected function determineExtension() {
    $extension = '';

    if($this->_options['force_extension'] != false) {
      $extension = $this->_options['force_extension'];
      if(substr($extension, 0, 1) !== '.') $extension = '.'.$extension;
    } elseif(($extension_orig = $this->file->getOriginalExtension(false)) && $this->_options['ignore_original_extension'] == false) {
      $extension = $extension_orig;
    } elseif($this->_options['do_guess_extension']) {
      $extension = $this->file->getExtension('');
    }

    return $extension;
  }

  /**
   * Checks if the given path does not exist in the S3 bucket
   *
   * @param string $path The proposed S3 URI
   * @return boolean
   */
  protected function isUniquePathRemote($path) {
    return false === S3::getObjectInfo($this->getAppConfig('s3_bucket'), $path, false);
  }

  /**
   * Checks if the given path does not exist in the 'path' column in the database.
   *
   * @param string $path The proposed S3 URI
   * @return boolean
   */
  protected function isUniquePathLocal($path) {
    $count = $this->getTable()
      ->createQuery('s')
      ->andWhere("s.{$this->getColumn('path')} = ?", $path)
      ->count();

    return $count === 0;
  }

  /**
   * Determines from the settings the S3 URI uniqueness criteria, and calls one or
   * both of the methods that check for uniqueness.
   *
   * @see self::isUniquePathLocal
   * @see self::isUniquePathRemote
   * @param <type> $path The proposed S3 URI
   * @return boolean True if S3 URI is unique, depending on settings
   */
  protected function isUniquePath($path) {
    if($this->_options['ensure_unique_local'] && false === $this->isUniquePathLocal($path)) {
      return false;
    }

    if($this->_options['ensure_unique_remote'] && false === $this->isUniquePathRemote($path)) {
      return false;
    }

    return true;
  }

  /**
   * Builds a path from given directory, base filename, and extension. Leading slash
   * is always removed from directory (S3 doesn't want it)
   * 
   * @param string $dir Directory (leading and trailing slashes don't matter)
   * @param string $base_filename Filename without extension
   * @param string $extension
   * @return string
   */
  protected static function getPathFromParts($dir, $base_filename, $extension) {
    return self::normalizeDir($dir, false, true) . $base_filename . $extension;
  }

  /**
   * The the leading and trailing slashes figured out
   *
   * @param string $dir The path to normalize
   * @param boolean $leading True adds/keeps the leading slash, false removes it
   * @param boolean $trailing True adds/keeps the trailing slash, false removes it
   * @param boolean $slash_if_empty If $dir was empty, return a slash?
   * @return string 
   */
  protected static function normalizeDir($dir, $leading=true, $trailing=false, $slash_if_empty=false)
  {
    $dir = trim($dir, '\\/');

    if(empty($dir))
      return ($slash_if_empty) ? '/' : '';
    if($leading)
      $dir = '/' . $dir;
    if($trailing)
      $dir .= '/';

    return $dir;
  }

  protected static function getRandomFilename() {
    return base_convert(sha1(uniqid(mt_rand(), true)), 16, 36);
  }


  /**
   * S3 doesn't support move ops, so copy to new path and delete the old one.
   *
   * @throws S3SaveableException on empty or non-unique path
   * @throws S3Exception on S3 copy or delete failure
   * @return Doctrine_Record
   */
  public function moveObject() {
    $invoker = $this->getInvoker();

    $path_column = $this->getColumn('path');
    $old_values = $invoker->getModified(true);

    if(false === array_key_exists($path_column, $old_values)) {
      return false;
    }

    $this->initializeS3();
    
    // trim off leading slash on new value
    $invoker[$path_column] = ltrim($invoker[$path_column], '\\/');
    
    $old_path = $old_values[$path_column];
    $new_path = $invoker[$path_column];
    $bucket = $this->getAppConfig('s3_bucket');

    if(empty($new_path)) {
      throw new S3SaveableException("New value for {$path_column} cannot be empty");
    }

    if(false === $this->isUniquePath($invoker[$this->getColumn('path')])) {
      throw new S3SaveableException('New S3 path was not unique');
    }

    // copy to new path
    $copy_success = S3::copyObject($bucket, $old_path, $bucket, $new_path, $this->_options['s3_acl']);

    if(false === $copy_success) {
      throw new S3Exception('Could not copy object to new path');
    }

    // delete old path
    if(false === S3::deleteObject($bucket, $old_path)) {
      // try deleting new path
      $delete_new_success = S3::deleteObject($bucket, $new_path);

      throw new S3Exception('Could not delete old object' . ($delete_new_success ? '' : ' or clean up the object at the new path'));
    }

    return $invoker;
  }

  /**
   * Delete the object from the invoker's S3 URI column
   * 
   * @return Doctrine_Record
   * @throws S3Exception if S3 DELETE fails
   */
  public function deleteObject() {
    $this->initializeS3();
    $invoker = $this->getInvoker();

    $bucket = $this->getAppConfig('s3_bucket');
    $path = $invoker[$this->getColumn('path')];

    if($path == false) {
      return false;
    }

    if(false === S3::deleteObject($bucket, $path)) {
      throw new S3Exception("S3 DELETE failed: could not delete S3 object ({$bucket}\{$path})");
    }

    $invoker[$this->getColumn('is_deleted')] = true;

    return $invoker;
  }

  /**
   * Download the S3 object from this record's S3 URI column to $path.
   *
   * @param string $path
   * @param boolean $overwrite If the file exists, overwrite it if true
   * @return Doctrine_Record
   * @throws S3SaveableException if directory or file is not writable, or when file exists and $overwrite is false
   * @throws S3Exception on S3 GET failure
   */
  public function downloadTo($path, $overwrite=false) {
    $invoker = $this->getInvoker();
    $this->initializeS3();

    $dir = dirname($path);
    $path = basename($path);

    if(false === is_writable($dir)) {
      throw new S3SaveableException('Directory is not writable');
    }

    if(file_exists($path)) {
      if(false == $overwrite) {
        throw new S3SaveableException('File already exists at this location');
      } elseif(false === is_writable($path)) {
        throw new S3SaveableException('File to overwrite is not writable');
      }
    }

    if(false === S3::getObject($this->getAppConfig('s3_bucket'), $invoker[$this->getColumn('path')], $path)) {
      throw new S3Exception('S3 GET failed');
    }

    return $invoker;
  }

  public function getPublicUrl($https=false) {
    $invoker = $this->getInvoker();
    return ($https ? 'https' : 'http') . '://s3.amazonaws.com/' . $this->getAppConfig('s3_bucket') . '/' . $invoker[$this->getColumn('path')];
  }
}
