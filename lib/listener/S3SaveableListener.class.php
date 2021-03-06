<?php

class S3SaveableListener extends Doctrine_Record_Listener
{
  public function preDelete(Doctrine_Event $event) {
    $event->getInvoker()->deleteObject();
  }

  public function preUpdate(Doctrine_Event $event) {
    $event->getInvoker()->moveObject();
  }

  public function preInsert(Doctrine_Event $event) {
    $event->getInvoker()->putObject();
  }
}