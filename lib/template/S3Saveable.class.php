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

    'ignore_original_extension'   => false, // test
    'do_guess_extension'          => true,  // test
    'force_extension'             => false, // test

    'delete_local_on_save'        => false,

    's3_dir'                      => false,
    's3_base_filename'            => false,
    's3_acl'                      => S3::ACL_PUBLIC_READ, // test

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

    $this->addListener(new adhS3SaveableListener($this->_options));
  }


  protected function initializeS3() {
    if(false === (self::$s3_instance instanceof S3)) {
      self::$s3_instance = new S3($this->getAppConfig('s3_access_key'), $this->getAppConfig('s3_secret_key'));
    }

    return self::$s3_instance;
  }

  protected function getAppConfig($type) {
    $key = $this->_options[$type.'_config_key'];

    if (false === ($val = sfConfig::get('app_'.$key, false))) {
      throw new S3Exception("Could not find configuration key in app.yml ({$type}_config_key => {$key})");
    }
    
    return $val;
  }

  protected function getColumn($key) {
    $column = $this->_options['columns'][$key];
    if(empty($column)) {
      throw new S3Exception("No column name given for {$column}");
    }

    return $column;
  }



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

  public function fromValidatedFile(sfValidatedFile $file, array $options=null) {
    if($options) {
      Doctrine_Lib::arrayDeepMerge($this->_options, $options);
    }

    $this->file = $file;

    return $this->getInvoker();
  }



  public function putObject() {
    if(false === $this->file instanceof sfValidatedFile) {
      return false;
    }

    $this->initializeS3();
    $invoker = $this->getInvoker();

    $path_original = $this->file->getTempName();
    $path_s3 = $this->determineS3Path();

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

  protected function determineS3Path() {
    $dir = $this->_options['s3_dir'] ? $this->_options['s3_dir'] : '';
    $base_filename = $this->_options['s3_base_filename'];
    $extension = $this->determineExtension();

    $custom_path = self::getPathFromParts($dir, $base_filename, $extension);
    if($base_filename && $this->isUniquePath($custom_path)) {
      return $custom_path;
    } elseif(false === $this->_options['generate_random_filename']) {
      $e_msg = 'Custom filename was required and ' . ($base_filename ? 'is not unique' : 'was not provided');
      throw new S3Exception($e_msg);
    }

    // continue generating a random filename until it meets uniqueness criteria
    $tries = 0;
    do {
      $base_filename = self::getRandomFilename();
      $path = self::getPathFromParts($dir, $base_filename, $extension);

      if($tries === 10) {
        throw new S3Exception('Could not find random unique filename after 10 tries');
      }
      ++$tries;
    } while(false === $this->isUniquePath($path));

    return $path;
  }

  protected function determineExtension() {
    $extension = '';

    if($this->_options['force_extension'] != false) {
      $extension = $this->_options['force_extension'];
    } elseif(($extension_orig = $this->file->getOriginalExtension(false)) && false == $this->_options['ignore_original_extension']) {
      $extension = $extension_orig;
    } elseif($this->_options['do_guess_extension']) {
      $extension = $this->file->getExtension('');
    }

    return $extension;
  }

  protected function isUniquePathRemote($path) {
    return false === S3::getObjectInfo($this->getAppConfig('s3_bucket'), $path, false);
  }

  protected function isUniquePathLocal($path) {
    $count = $this->getTable()
      ->createQuery('s')
      ->andWhere("s.{$this->getColumn('path')} = ?", $path)
      ->count();

    return $count === 0;
  }

  protected function isUniquePath($path) {
    if($this->_options['ensure_unique_local'] && false === $this->isUniquePathLocal($path)) {
      return false;
    }

    if($this->_options['ensure_unique_remote'] && false === $this->isUniquePathRemote($path)) {
      return false;
    }

    return true;
  }

  protected static function getPathFromParts($dir, $base_filename, $extension) {
    return self::normalizeDir($dir, false, true) . $base_filename . $extension;
  }

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
   * @return <type> 
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
      throw new S3Exception("New value for {$path_column} cannot be empty");
    }

    if(false === $this->isUniquePath($invoker[$this->getColumn('path')])) {
      throw new S3Exception('New S3 path was not unique');
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

  public function deleteObject() {
    $this->initializeS3();
    $invoker = $this->getInvoker();

    $bucket = $this->getAppConfig('s3_bucket');
    $path = $invoker[$this->getColumn('path')];

    if($path == false) {
      return false;
    }

    if(false === S3::deleteObject($bucket, $path)) {
      throw new S3Exception("Could not delete S3 object ({$bucket}\{$path})");
    }

    $invoker[$this->getColumn('is_deleted')] = true;

    return $invoker;
  }

  public function downloadTo($path, $overwrite=false) {
    $invoker = $this->getInvoker();
    $this->initializeS3();

    $dir = dirname($path);
    $path = basename($path);

    if(false === is_writable($dir)) {
      throw new S3Exception('Directory is not writable');
    }

    if(file_exists($path)) {
      if(false == $overwrite) {
        throw new S3Exception('File already exists at this location');
      } elseif(false === is_writable($path)) {
        throw new S3Exception('File to overwrite is not writable');
      }
    }

    S3::getObject($this->getAppConfig('s3_bucket'), $invoker[$this->getColumn('path')], $path);

    return $invoker;
  }
}